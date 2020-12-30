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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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
	log         []LogEntry    // log[0]为dummy节点
	commitIndex int           // 提交的最大索引
	lastApplied int           // 已应用到本地状态机的最大索引
	applyCh     chan ApplyMsg // 给channel发ApplyMsg消息以应用日志项到本地状态机
	nextIndex   []int         // leader用，nextIndex[i]是准备发往follower[i]的起始索引，每次选举后重置
	matchIndex  []int         // leader用，matchIndex[i]是已同步到follower[i]的最大索引，每次选举后重置
}

func (rf *Raft) String() string {
	return fmt.Sprintf("n%d{t%d}", rf.me, rf.currentTerm)
}

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

// 论文图4的server状态机，三个状态的转化
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

// CANDIDATE
func (rf *Raft) becomeCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++    // increment currentTerm
	rf.votedFor = rf.me // vote for self
	rf.resetElectionTimer()
	DPrintf("%v now CANDIDATE\n", rf)
}

func (rf *Raft) askForVote() {
	// send RequestVote RPCs to all other servers
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[lastLogIndex].Term,
		LastLogIndex: lastLogIndex,
	}

	totalVotes := len(rf.peers)
	voteCh := make(chan bool, totalVotes-1)
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				DPrintf("n%d{t%d}-reqvot->n%d\n", args.CandidateId, args.Term, i)
				if ok := rf.sendRequestVote(i, &args, &reply); !ok {
					voteCh <- false
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 见到更大任期变follower
				if reply.Term > args.Term {
					rf.becomeFollower(reply.Term)
				}
				// 距离收到reply有一段时间，要检查当初的请求主体还在不在
				if rf.role != CANDIDATE || rf.currentTerm != args.Term {
					voteCh <- false
					return
				}

				voteCh <- reply.VoteGranted
			}(i)
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
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is
	//    at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		if isLogAtLeastAsUpToDateAs(args.LastLogTerm, args.LastLogIndex,
			rf.log[lastLogIndex].Term, lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			reply.VoteGranted = true
			DPrintf("%v vote n%d{t%d}", rf, args.CandidateId, args.Term)
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
	rf.votedFor = -1
	rf.resetElectionTimer()
	DPrintf("%v now FOLLOWER\n", rf)
}

func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(randIntBetween(300, 500)) * time.Millisecond
	rf.timer.Reset(timeout)
}

func randIntBetween(lower int, upper int) int {
	rand.Seed(time.Now().UnixNano())
	return lower + rand.Intn(upper-lower)
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
	interval := 100 * time.Millisecond
	rf.timer.Reset(interval)
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			// leader用参数prevLogIndex来向follower探查最后一个匹配日志项的索引
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}

			go func(i int) {
				DPrintf("n%d{t%d}-ping->n%d\n", args.LeaderId, args.Term, i)
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 见到更大任期变follower
				if reply.Term > args.Term {
					rf.becomeFollower(reply.Term)
				}
				// 距离收到reply有一段时间，要检查当初的请求主体还在不在
				if rf.role != LEADER || rf.currentTerm != args.Term {
					return
				}

				if reply.Success { // follower包含匹配的日志项
					rf.matchIndex[i] = prevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.updateCommitIndex()
				} else {
					rf.nextIndex[i]--
				}
			}(i)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// 拷贝matchIndex[]后逆序排列
	size := len(rf.matchIndex)
	arr := make([]int, size)
	copy(arr, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(arr)))
	// 去掉结尾的leader对应值0（leader值可以认为足够大）
	// 再去掉前面idx个大数，假设所求索引为arr[idx:-1]，
	// 要满足：idx+1+1（前面idx个大数+1个leader+自身）> len(arr)/2（即majority）
	// idx >= len(arr)/2-1
	arr = arr[len(arr)/2-1 : len(arr)-1]

	for _, N := range arr {
		if N <= rf.commitIndex {
			return
		}
		if rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			DPrintf("%v commit log{%d}\n", rf, N)
			rf.applyCommitted()
			return
		}
	}
}

func (rf *Raft) applyCommitted() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
		DPrintf("%v apply log{%d}\n", rf, rf.lastApplied)
	}
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
}

// 处理leader的AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v handle n%d{t%d} AppendEntries", rf, args.LeaderId, args.Term)

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// 见到更大或相等的任期变follower
	rf.becomeFollower(args.Term)
	reply.Term = args.Term

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	//    whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 3. If an existing entry conflicts with a new one (same index but
		//    different terms), delete the existing entry and all that follow it
		if args.PrevLogIndex < len(rf.log) {
			rf.log = rf.log[0:args.PrevLogIndex]
		}
		return
	}
	reply.Success = true

	// 4. Append any new entries not already in the log
	rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)

	// follower更新commitIndex
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//    min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCommitted()
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()
	go rf.serverLoop()

	return rf
}
