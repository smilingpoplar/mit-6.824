package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	role        int         // 初始为FOLLOWER
	currentTerm int         // 所知的最大任期号，初始为0
	votedFor    int         // 当前任期的票已投给哪个candidate，初始为-1
	timer       *time.Timer // leader的心跳定时器，或者follower/candidate的选举定时器
	// 2B
	log          []LogEntry    // log[0]为dummy节点
	commitIndex  int           // 提交的最大索引
	lastApplied  int           // 已应用到本地状态机的最大索引
	applyCh      chan ApplyMsg // 给channel发ApplyMsg消息以应用日志项到本地状态机
	applyCond    *sync.Cond    // 可以将(lastApplied,commitIndex]间的日志项应用到本地状态机了
	applyPending bool          // 在应用日志项时又来了新的apply，配合applyCond使用
	nextIndex    []int         // leader用，nextIndex[i]是准备发往follower[i]的起始索引，每次选举后重置
	matchIndex   []int         // leader用，matchIndex[i]是已同步到follower[i]的最大索引，每次选举后重置
}

func (rf *Raft) String() string {
	star := ""
	if rf.role == LEADER {
		star = "*"
	}
	return fmt.Sprintf("s%d{t%d}%v", rf.me, rf.currentTerm, star)
}

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

func (rf *Raft) serverLoop() {
	for !rf.killed() {
		<-rf.timer.C
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.becomeCandidate()
			rf.askForVote()
		} else {
			rf.resetHeartbeatTimer()
			rf.sendHeartbeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.applyPending {
			rf.applyCond.Wait()
			rf.applyPending = false
		}
		rf.mu.Unlock()
		rf.applyEntries()
	}
}

func (rf *Raft) applyEntries() {
	// 在锁内往applyCh发消息可能死锁，要在锁外发
	msgs := make([]ApplyMsg, 0)
	rf.mu.Lock()
	if Debug > 0 && rf.lastApplied < rf.commitIndex {
		DPrintf("%v apply {%d}", rf, rf.commitIndex)
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msgs = append(msgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		})
	}
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
	}
}

// CANDIDATE
func (rf *Raft) becomeCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++    // increment currentTerm
	rf.votedFor = rf.me // vote for self
	rf.persist()
	rf.resetElectionTimer()
	DPrintf("%v now CANDIDATE\n", rf)
}

func (rf *Raft) askForVote() {
	// send RequestVote RPCs to all other servers
	totalVotes := len(rf.peers)
	voteCh := make(chan bool, totalVotes-1)
	for i := range rf.peers {
		if i != rf.me {
			rf.doRequestVote(i, voteCh)
		}
	}

	go func() {
		// if votes received from majority of servers: become leader
		votesReceived := 1 // self vote
		for i := 0; i < cap(voteCh); i++ {
			if granted := <-voteCh; granted {
				votesReceived++
				if votesReceived > totalVotes/2 {
					DPrintf("%v got >=%v votes\n", rf, votesReceived)
					rf.mu.Lock()
					rf.becomeLeader()
					rf.mu.Unlock()
					return
				}
			}
		}
	}()
}

func (rf *Raft) doRequestVote(i int, voteCh chan bool) {
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[lastLogIndex].Term,
		LastLogIndex: lastLogIndex,
	}

	go func() {
		reply := RequestVoteReply{}
		DPrintf("s%d{t%d}-reqvot->s%d\n", args.CandidateId, args.Term, i)
		if ok := rf.sendRequestVote(i, &args, &reply); !ok {
			voteCh <- false
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 见到更大任期变follower
		rf.becomeFollowerIfSeenLargerTerm(reply.Term)
		// 距离收到reply有一段时间，要检查当初的请求主体还在不在
		if rf.role != CANDIDATE || rf.currentTerm != args.Term {
			voteCh <- false
			return
		}

		voteCh <- reply.VoteGranted
	}()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate的任期号
	CandidateId int
	// 2B
	LastLogIndex int // candidate最后日志项的索引
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 对方的currentTerm
	VoteGranted bool // 是否收到投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理candidate的RequestVote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = args.Term
	// 见到更大任期变follower
	rf.becomeFollowerIfSeenLargerTerm(args.Term)
	// 2. If votedFor is null or candidateId, and candidate’s log is
	//    at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		if isLogAtLeastAsUpToDateAs(args.LastLogTerm, args.LastLogIndex,
			rf.log[lastLogIndex].Term, lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.resetElectionTimer()
			reply.VoteGranted = true
			DPrintf("%v vote s%d{t%d}", rf, args.CandidateId, args.Term)
		}
	}
}

// 比较candidate和receiver的{term, logIndex}
func isLogAtLeastAsUpToDateAs(cTerm int, cIndex int, rTerm int, rIndex int) bool {
	if cTerm == rTerm {
		return cIndex >= rIndex
	}
	return cTerm > rTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// FOLLOWRER
func (rf *Raft) becomeFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(randIntBetween(150, 300)) * time.Millisecond
	rf.timer.Reset(timeout)
}

func randIntBetween(lower int, upper int) int {
	rand.Seed(time.Now().UnixNano())
	return lower + rand.Intn(upper-lower)
}

func (rf *Raft) becomeFollowerIfSeenLargerTerm(term int) {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.becomeFollower(term)
	}
}

// LEADER
func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	rf.resetHeartbeatTimer()
	DPrintf("%v now LEADER\n", rf)
	// 2B
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) resetHeartbeatTimer() {
	interval := 50 * time.Millisecond
	rf.timer.Reset(interval)
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			rf.doAppendEntries(i)
		}
	}
}

func (rf *Raft) doAppendEntries(i int) {
	// DPrintf("s%d{t%d}-ping->s%d\n", args.LeaderId, args.Term, i)
	// 用prevLogIndex探查与follower最后匹配的日志项位置
	prevLogIndex := rf.nextIndex[i] - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[rf.nextIndex[i]:],
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 见到更大任期变follower
		rf.becomeFollowerIfSeenLargerTerm(reply.Term)
		// 距离收到reply有一段时间，要检查当初的请求主体还在不在
		if rf.role != LEADER || rf.currentTerm != args.Term {
			return
		}

		if reply.Success { // follower包含匹配的日志项
			rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			rf.updateCommitIndex()
		} else {
			// 2C，回退优化
			if reply.ConflictTerm != -1 {
				conflictTermIndex := -1 // 找conflictTerm的最后位置
				for i := args.PrevLogIndex; i > 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						conflictTermIndex = i
						break
					}
				}
				if conflictTermIndex != -1 { // 找到
					rf.nextIndex[i] = conflictTermIndex + 1
				} else {
					rf.nextIndex[i] = reply.ConflictIndex
				}
			} else { // 尝试比较的prevLogIndex位置超出日志长
				rf.nextIndex[i] = reply.ConflictIndex
			}
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	arr := make([]int, len(rf.matchIndex))
	copy(arr, rf.matchIndex)
	arr[rf.me] = len(rf.log) - 1 // leader自身
	sort.Sort(sort.Reverse(sort.IntSlice(arr)))
	arr = arr[len(arr)/2:]

	for _, N := range arr {
		if N <= rf.commitIndex {
			return
		}
		if rf.log[N].Term == rf.currentTerm {
			rf.setCommitIndex(N)
			DPrintf("%v commit {%d}\n", rf, N)
			return
		}
	}
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	rf.applyPending = true
	rf.applyCond.Broadcast()
}

// AppendEntries请求作为心跳
type AppendEntriesArgs struct {
	Term     int // leader的任期号
	LeaderId int
	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // 要复制的日志项
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // 对方的currentTerm
	Success bool // follower包含匹配prevLogIndex和prevLogTerm的日志项
	// 2C，优化
	ConflictIndex int
	ConflictTerm  int
}

// 处理leader的AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v handle s%d{t%d} AppendEntries", rf, args.LeaderId, args.Term)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// 见到更大或相等的任期变follower
	rf.becomeFollowerIfSeenLargerTerm(args.Term)
	if args.Term == rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = args.Term

	// 日志项更新策略
	// leader从后往前尝试prevLogIndex，希望在peer[i]的prevLogIndex处找到和leader任期相同的日志项
	// * 找到了就把>prevLogIndex位置的日志项丢掉，拼上leader发来的新entries
	// * 找不到就把冲突位置设为conflictIndex，返回给leader设置nextIndex=conflictIndex，
	//   leader下次再从prevLogIndex=nextIndex-1=conflictIndex-1处继续发心跳试探

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	//    whose term matches prevLogTerm
	// 2C，回退优化，https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}
	prevLogTerm := rf.log[args.PrevLogIndex].Term
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		// 找第一个conflictTerm的位置
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term == prevLogTerm {
				reply.ConflictIndex = i
			} else {
				break
			}
		}
		return
	}

	reply.Success = true
	// 3. If an existing entry conflicts with a new one (same index but
	//    different terms), delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	// 注意if的表述，即使follower含有AppendEntries请求的所有entries，follower中entries后的日志项也不能截掉。
	// 因为AppendEntries可能乱序到底，旧请求可能后到达，若截掉等于已告诉leader拥有的日志项消失了。
	entries := make([]LogEntry, len(rf.log)) // rf.log上有DATA RACE，用临时变量entries
	copy(entries, rf.log)
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index >= len(entries) {
			entries = append(entries, entry)
		} else {
			if entries[index].Term != entry.Term {
				entries = entries[:index]
				entries = append(entries, entry)
			}
		}
	}
	rf.log = entries
	rf.persist()

	// 已提交的entries是不会被截掉的，commitIndex单调递增
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//    min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, len(rf.log)-1))
		DPrintf("%v commit {%d}\n", rf, rf.commitIndex)
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// leader发送AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == LEADER {
		isLeader = true
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{command, term})
		rf.persist()
	}

	return index, term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatalf("%v encode currentTerm error: %v", rf, err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatalf("%v encode votedFor error: %v", rf, err)
	}
	if err := e.Encode(rf.log); err != nil {
		log.Fatalf("%v encode log error: %v", rf, err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		log.Fatalf("%v decode currentTerm error: %v", rf, err)
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatalf("%v decode votedFor error: %v", rf, err)
	}
	if err := d.Decode(&entries); err != nil {
		log.Fatalf("%v decode log error: %v", rf, err)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = entries
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timer = time.NewTimer(time.Second)
	// 2B
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()
	go rf.serverLoop()
	go rf.applyLoop()

	return rf
}
