package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // Get/Put/Append
	Key   string
	Value string
	RequestId
}

type OpDone struct {
	err   Err
	value string
	RequestId
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store    map[string]string
	opDoneCh map[int]chan OpDone // logIndex => opDoneCh
	opDoneMu sync.Mutex
	dedup    map[int]int // clientId => lastSeqNum
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: GET, Key: args.Key, RequestId: args.RequestId}
	reply.Value, reply.Err = kv.awaitOp(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId}
	_, reply.Err = kv.awaitOp(op)
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
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.opDoneCh = make(map[int]chan OpDone)
	kv.dedup = make(map[int]int)

	go kv.applyLoop()

	return kv
}

func (kv *KVServer) awaitOp(op Op) (string, Err) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}

	ch := make(chan OpDone, 1) // 不阻塞
	kv.opDoneMu.Lock()
	kv.opDoneCh[index] = ch
	kv.opDoneMu.Unlock()
	defer kv.delOpDoneCh(index)

	select {
	case d := <-ch:
		if d.RequestId != op.RequestId {
			return "", ErrWrongLeader
		}
		return d.value, d.err
	case <-time.After(time.Second): // 超时返回
		return "", ErrWrongLeader
	}
}

func (kv *KVServer) delOpDoneCh(index int) {
	kv.opDoneMu.Lock()
	delete(kv.opDoneCh, index)
	kv.opDoneMu.Unlock()
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			op := msg.Command.(Op)
			d := OpDone{err: OK, RequestId: op.RequestId}
			kv.mu.Lock()
			if op.Type == GET {
				kv.applyGet(&op, &d)
			} else {
				kv.applyPutAppend(&op)
			}
			kv.mu.Unlock()

			kv.opDoneMu.Lock()
			ch, ok := kv.opDoneCh[msg.CommandIndex]
			kv.opDoneMu.Unlock()
			if ok {
				ch <- d
			}
		}
	}
}

func (kv *KVServer) applyGet(op *Op, d *OpDone) {
	val, ok := kv.store[op.Key]
	if ok {
		d.value = val
	} else {
		d.err = ErrNoKey
	}
}

func (kv *KVServer) applyPutAppend(op *Op) {
	if op.SeqNum <= kv.dedup[op.ClientId] {
		return
	}
	if op.Type == PUT {
		kv.store[op.Key] = op.Value
	} else if op.Type == APPEND {
		kv.store[op.Key] += op.Value
	}
	kv.dedup[op.ClientId] = op.SeqNum
}
