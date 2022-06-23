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
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	role        Role        // 初始为follower
	currentTerm int         // 所知的最大任期
	votedFor    int         // 当前任期的票已投给哪个candidate，初始为-1
	timer       *time.Timer // leader的心跳定时器，或者follower/candidate的选举定时器
	// 2B
	log         []LogEntry    // log[0]为dummy节点
	commitIndex int           // 已复制到>1/2服务器的log[]最大索引
	lastApplied int           // 已应用到状态机的log[]最大索引
	nextIndex   []int         // leader用，nextIndex[i]是将要复制到follower[i]的起始索引，每次选举后重置
	matchIndex  []int         // leader用，matchIndex[i]是已复制到follower[i]的最大索引，每次选举后重置
	applyCh     chan ApplyMsg // 给applyCh发消息以应用LogEntry到状态机
	applyCond   *sync.Cond
}

func (rf *Raft) String() string {
	star := ""
	if rf.role == Leader {
		star = "*"
	}
	return fmt.Sprintf("n%d{t%d}%v", rf.me, rf.currentTerm, star)
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return []string{"FOLLOWER", "CANDIDATE", "LEADER"}[r]
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate的currentTerm
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
	Term        int  // currentTerm，candidate用来更新自己的currentTerm
	VoteGranted bool // 是否投给了candidate
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理来自candidate的投票请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("n%d{t%d}-reqvot->%v\n", args.CandidateId, args.Term, rf)

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = args.Term
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is
	//    at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 5.4.1 election restriction
		// Raft uses the voting process to prevent a candidate from
		// winning an election unless its log contains all committed
		// entries. A candidate must contact a majority of the cluster
		// in order to be elected, which means that every committed
		// entry must be present in at least one of those servers. If the
		// candidate’s log is at least as up-to-date as any other log
		// in that majority, then it will hold all the committed entries.
		if lastLogIndex := len(rf.log) - 1; atLeastUpToDate(args.LastLogTerm, args.LastLogIndex,
			rf.log[lastLogIndex].Term, lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			reply.VoteGranted = true
			DPrintf("%v vote n%d{t%d}", rf, args.CandidateId, args.Term)
		}
	}
}

// 比较candidate和receiver的{logTerm,logIndex}
func atLeastUpToDate(cTerm int, cIndex int, rTerm int, rIndex int) bool {
	if cTerm == rTerm {
		return cIndex >= rIndex
	}
	return cTerm > rTerm
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

	if rf.role == Leader {
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		rf.log = append(rf.log, LogEntry{command, term})
		rf.sendAppendEntriesToPeers()
	}

	return index, term, isLeader
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
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timer = time.NewTimer(time.Second)
	rf.resetElectionTimer()
	// 2B
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serverLoop()
	go rf.applyLoop()

	return rf
}

func (rf *Raft) serverLoop() {
	for !rf.killed() {
		<-rf.timer.C
		rf.mu.Lock()
		if rf.role != Leader {
			rf.beCandidate()
			rf.sendRequestVoteToPeers()
		} else {
			rf.resetHeartbeatTimer()
			rf.sendAppendEntriesToPeers()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) beFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
		DPrintf("%v now %v\n", rf, rf.role)
	}
	rf.currentTerm = term
	rf.resetElectionTimer()
}

func (rf *Raft) resetElectionTimer() {
	rf.timer.Reset(randMillisecondBetween(300, 500))
}

func randMillisecondBetween(lower int, upper int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := lower + r.Intn(upper-lower)
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) beCandidate() {
	rf.role = Candidate
	rf.currentTerm++    // increment currentTerm
	rf.votedFor = rf.me // vote for self
	rf.resetElectionTimer()
	DPrintf("%v now %v\n", rf, rf.role)
}

func (rf *Raft) sendRequestVoteToPeers() {
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[lastLogIndex].Term,
		LastLogIndex: lastLogIndex,
	}

	var votes int32 = 1 // self vote
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &args, &reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					return
				}
				// 收到reply隔了一段时间，要检查当初的请求主体还在不在
				if rf.role != Candidate || rf.currentTerm != args.Term {
					return
				}

				if reply.VoteGranted {
					v := atomic.LoadInt32(&votes)
					v++
					atomic.StoreInt32(&votes, v)
					// received from majority of servers: become leader
					if int(v) == len(rf.peers)/2+1 {
						DPrintf("%v got %d votes\n", rf, v)
						rf.beLeader()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) beLeader() {
	rf.role = Leader
	rf.resetHeartbeatTimer()
	DPrintf("%v now %v\n", rf, rf.role)
	// 2B
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.timer.Reset(100 * time.Millisecond)
}

func (rf *Raft) sendAppendEntriesToPeers() {
	// leader发送心跳
	for i := range rf.peers {
		if i != rf.me {
			// 用prevLogIndex探查leader与follower[i]日志匹配的最大索引lastMatch
			prevLogIndex := rf.nextIndex[i] - 1
			// DATA RACE：
			// 若本leader收到了其他leader心跳，则sendAppendEntries()=>labrpc=>labgob序列化参数时读rf.log，
			// 与后面AppendEntries()中写rf.log冲突，sendAppendEntries()长时调用不能加锁，故拷贝一份entries。
			entries := rf.log[rf.nextIndex[i]:]
			entriesCopy := make([]LogEntry, len(entries))
			copy(entriesCopy, entries)

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      entriesCopy,
				LeaderCommit: rf.commitIndex,
			}

			go func(follower int) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(follower, &args, &reply); !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					return
				}
				// 收到reply隔了一段时间，要检查当初的请求主体还在不在
				if rf.role != Leader || rf.currentTerm != args.Term {
					return
				}

				if reply.Success { // follower包含匹配的日志项
					rf.matchIndex[follower] = prevLogIndex + len(args.Entries)
					rf.nextIndex[follower] = rf.matchIndex[follower] + 1
					rf.updateCommitIndex()
				} else { // 回退
					if rf.nextIndex[follower] > 1 { // log[0]为dummy节点
						rf.nextIndex[follower]--
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	//
	// 从matchIndex[]中找到">1/2多数"的那个数，假设所求的数为下标idx，
	// 拷贝后逆序排列，去掉结尾的leader对应值0（leader值可以认为足够大），再去掉前面idx个大数，
	// 要满足：idx+1+1（前面idx个大数+第idx数+1个leader）> len(arr)/2
	// idx >= len(arr)/2-1
	arr := make([]int, len(rf.matchIndex))
	copy(arr, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(arr)))
	arr = arr[len(arr)/2-1 : len(arr)-1] // arr是可满足条件的数集

	for _, N := range arr {
		if N <= rf.commitIndex {
			return
		}
		if rf.log[N].Term == rf.currentTerm {
			rf.setCommitIndex(N)
			return
		}
	}
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	DPrintf("%v commit log{%d}\n", rf, commitIndex)
	rf.applyCond.Signal()
}

type AppendEntriesArgs struct {
	Term     int // leader的currentTerm
	LeaderId int
	// 2B
	// 通过心跳复制leader日志到follower[i]，关键是找到leader与follower[i]日志匹配的最大索引lastMatch，然后将follower[i]的log[lastMatch+1:]全部覆盖。
	// leader用prevLogIndex从最后位置（nextIndex[i]-1）往前探查lastMatch。日志匹配，这里检查某index位置的term相同。
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // leader比follower[i]多的日志项
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int // 心跳接收方的currentTerm，leader用来更新自己的currentTerm
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 处理来自leader的心跳
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("n%d{t%d}*-ping->%v\n", args.LeaderId, args.Term, rf)

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	// 面对leader，即使相同的term也变follower
	rf.beFollower(args.Term)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	//    whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	// 至此args.PrevLogIndex是自身和leader日志的最后匹配位置
	// 直接rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)截断log[]是错的，
	// 因为心跳可能乱序到达，若新心跳拼上较长entries[]后旧心跳到达，会截断再拼上较短entries[]，日志丢失

	reply.Success = true
	// 3. If an existing entry conflicts with a new one (same index but
	//    different terms), delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index] // term冲突时才截断
				rf.log = append(rf.log, entry)
			}
		}
	}

	// 根据leader的commitIndex更新自己的
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//    min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, len(rf.log)-1))
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		msgs := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		if len(msgs) > 0 {
			DPrintf("%v apply log{%d}", rf, rf.commitIndex)
		}
		rf.mu.Unlock()

		// 在锁外给applyCh发消息
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		time.Sleep(25 * time.Millisecond) // 防止太频繁
	}
}
