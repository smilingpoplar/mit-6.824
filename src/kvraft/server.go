package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string // Get/Put/Append
	Key    string
	Value  string
	RequestId
}

// Op传给raft，从applyCh返回OpResult
type OpResult struct {
	op    Op
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store      map[string]string
	opResultCh map[int]chan OpResult // 监听logIndex处的日志项apply完成
	lastSeqNum map[int]int           // client最新请求的seqNum
	persister  *raft.Persister
}

func (kv *KVServer) sendToRaft(op Op) OpResult {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{err: ErrWrongLeader}
	}

	ch := kv.getOpResultCh(index)
	defer kv.delOpResultCh(index)
	// leader可能因为网络分区，无法将log同步到majority
	// <-ch会阻塞，要加上超时，超时时没获得majority
	select {
	case result := <-ch:
		if result.op.RequestId != op.RequestId {
			return OpResult{err: ErrWrongLeader}
		}
		return result
	case <-time.After(time.Second):
		return OpResult{err: ErrWrongLeader}
	}
}

func (kv *KVServer) getOpResultCh(index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.opResultCh[index]
	if !ok {
		// 每个logIndex上至多1个消息，不阻塞生产者applyLoop
		ch = make(chan OpResult, 1)
		kv.opResultCh[index] = ch
	}
	return ch
}

func (kv *KVServer) delOpResultCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opResultCh, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Method: "Get", Key: args.Key, RequestId: args.RequestId}
	result := kv.sendToRaft(op)
	if result.err != "" {
		reply.Err = result.err
		return
	}

	reply.Value = result.value
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Method: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId}
	if result := kv.sendToRaft(op); result.err != "" {
		reply.Err = result.err
		return
	}

	reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.store = make(map[string]string)
	kv.opResultCh = make(map[int]chan OpResult)
	kv.lastSeqNum = make(map[int]int)

	// crash后从快照恢复
	kv.loadSnapshot(kv.persister.ReadSnapshot())

	go kv.applyLoop()

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			result := OpResult{op: op}

			kv.mu.Lock()
			if op.Method != "Get" {
				// apply日志项到本地状态机
				lastSeqNum, ok := kv.lastSeqNum[op.ClientId]
				if !ok || op.SeqNum > lastSeqNum {
					switch op.Method {
					case "Put":
						kv.store[op.Key] = op.Value
					case "Append":
						kv.store[op.Key] += op.Value
					}
					kv.lastSeqNum[op.ClientId] = op.SeqNum
				}
			} else {
				value, has := kv.store[op.Key]
				if !has {
					result.err = ErrNoKey
				} else {
					result.value = value
				}
			}
			kv.mu.Unlock()

			kv.tryTakeSnapshot(msg.CommandIndex)

			ch := kv.getOpResultCh(msg.CommandIndex)
			ch <- result
		} else {
			kv.loadSnapshot(msg.Command.([]byte))
		}
	}
}

func (kv *KVServer) tryTakeSnapshot(lastIncludedIndex int) {
	if kv.maxraftstate == -1 ||
		kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.store)
	e.Encode(kv.lastSeqNum)
	kv.mu.Unlock()
	snapshot := w.Bytes()
	// 当前状态snapshot，以及当前状态对应的索引lastIncludedIndex
	kv.rf.TakeSnapshot(snapshot, lastIncludedIndex)
}

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var lastSeqNum map[int]int
	if err := d.Decode(&store); err != nil {
		log.Fatalf("S%d decode store error: %v", kv.me, err)
	}
	if err := d.Decode(&lastSeqNum); err != nil {
		log.Fatalf("S%d decode lastSeqNum error: %v", kv.me, err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = store
	kv.lastSeqNum = lastSeqNum
}
