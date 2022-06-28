package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const debug = false

func (kv *ShardKV) Debug(format string, a ...interface{}) {
	if debug {
		log.Printf("%v "+format, append([]interface{}{kv}, a...)...)
	}
}

func (kv *ShardKV) DebugL(format string, a ...interface{}) {
	if kv.isLeader() {
		kv.Debug(format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // Get/Put/Append
	Key       string
	Value     interface{}
	ConfigNum int
	RequestId
}

func (op Op) String() string {
	str := fmt.Sprintf("{%v", op.Type)
	switch op.Type {
	case GET:
		str += fmt.Sprintf(" <=s%d: %v", key2shard(op.Key), op.Key)
	case PUT, APPEND:
		str += fmt.Sprintf(" =>s%d: %v %v", key2shard(op.Key), op.Key, op.Value)
	case CONFIG:
		str += fmt.Sprintf(": %d %v", op.Value.(shardctrler.Config).Num, op.Value.(shardctrler.Config).Shards)
	case PULL:
		str += fmt.Sprintf(": %v", op.Value.(Shard))
	case DONEPUSH:
		str += fmt.Sprintf(": s[%d:%v]", op.Value.(DoneMigrateArgs).Gid, op.Value.(DoneMigrateArgs).Ids)
	}
	str += "}"
	return str
}

type OpDone struct {
	err   Err
	value string
	RequestId
}

type Shard struct {
	Data      map[string]string
	Ids       []int
	Gid       int
	ConfigNum int
	Servers   []string
	Dedup     map[int]int
}

func (s Shard) String() string {
	str := fmt.Sprintf("s[%d:%v], v%d", s.Gid, s.Ids, s.ConfigNum)
	if len(s.Data) > 0 {
		str += fmt.Sprintf(", %v", s.Data)
	}
	return str
}

type ShardState int

const (
	ShardServing ShardState = iota
	ShardPushing
	ShardPulling
)

func (s ShardState) String() string {
	return []string{"Serving", "Pushing", "Pulling"}[s]
}

// 复用下面的dedup映射
const (
	DedupBasePull     = 1e4
	DedupBaseDonePush = 2 * 1e4
	DedupBaseClient   = 1e6
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       map[string]string
	opDoneCh    map[int]chan OpDone // logIndex => opDoneCh
	opDoneMu    sync.Mutex
	dedup       map[int]int // clientId => lastSeqNum
	persister   *raft.Persister
	lastApplied int
	dead        int32 // set by Kill()
	cck         *shardctrler.Clerk
	config      shardctrler.Config
	lastConfig  shardctrler.Config
	shardState  [shardctrler.NShards]ShardState
}

func (kv *ShardKV) String() string {
	star := " "
	if kv.isLeader() {
		star = "*"
	}
	return fmt.Sprintf("[g%d:n%d:v%d]%v", kv.gid, kv.me, kv.config.Num, star)
}

func (kv *ShardKV) migratingDebugStr() string {
	in, out := make(map[int][]int), make(map[int][]int)
	for id, state := range kv.shardState {
		switch state {
		case ShardPulling:
			from := kv.lastConfig.Shards[id]
			in[from] = append(in[from], id)
		case ShardPushing:
			to := kv.config.Shards[id]
			out[to] = append(out[to], id)
		}
	}

	str := ""
	if len(in) > 0 {
		str += fmt.Sprintf(" %v: s%v, v%d", ShardPulling, fmt.Sprint(in)[3:], kv.config.Num)
	}
	if len(out) > 0 {
		str += fmt.Sprintf(" %v: s%v, v%d", ShardPushing, fmt.Sprint(out)[3:], kv.config.Num)
	}
	if len(kv.store) > 0 {
		str += fmt.Sprintf("\n%v", kv.store)
	}
	return str
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value, reply.Err = kv.awaitOp(Op{Type: GET, Key: args.Key, ConfigNum: args.ConfigNum, RequestId: args.RequestId})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.awaitOp(Op{Type: args.Op, Key: args.Key, Value: args.Value, ConfigNum: args.ConfigNum, RequestId: args.RequestId})
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.store = make(map[string]string)
	kv.opDoneCh = make(map[int]chan OpDone)
	kv.dedup = make(map[int]int)
	kv.persister = persister

	// crash后从快照恢复
	kv.loadSnapshot(kv.persister.ReadSnapshot())

	go kv.applyLoop()

	// Use something like this to talk to the shardctrler:
	kv.cck = shardctrler.MakeClerk(kv.ctrlers)
	labgob.Register(Shard{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(DoneMigrateArgs{})

	go kv.configLoop()

	return kv
}

func (kv *ShardKV) awaitOp(op Op) (string, Err) {
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

func (kv *ShardKV) delOpDoneCh(index int) {
	kv.opDoneMu.Lock()
	delete(kv.opDoneCh, index)
	kv.opDoneMu.Unlock()
}

func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		kv.mu.Lock()
		if msg.SnapshotValid {
			if msg.SnapshotIndex > kv.lastApplied {
				kv.loadSnapshot(msg.Snapshot)
				// kv.Debug("load snapshot log{%d} %v", msg.SnapshotIndex, kv.migratingDebugStr())
				kv.lastApplied = msg.SnapshotIndex
			}
		} else if msg.CommandValid {
			if msg.CommandIndex > kv.lastApplied {
				op := msg.Command.(Op)
				d := OpDone{err: OK, RequestId: op.RequestId}
				if op.Type == CONFIG {
					kv.applyConfig(&op)
				} else if op.Type == PULL {
					kv.applyPull(&op, &d)
				} else if op.Type == DONEPUSH {
					kv.applyDonePush(&op, &d)
				} else if op.Type == GET {
					kv.applyGet(&op, &d)
				} else {
					kv.applyPutAppend(&op, &d)
				}
				kv.trySnapshot(msg.CommandIndex)
				kv.lastApplied = msg.CommandIndex

				kv.opDoneMu.Lock()
				ch, ok := kv.opDoneCh[msg.CommandIndex]
				kv.opDoneMu.Unlock()
				if ok {
					ch <- d
				}
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyGet(op *Op, d *OpDone) {
	if d.err = kv.checkOp(op); d.err != OK {
		// kv.DebugL("reject %v %v", op, d.err)
		return
	}
	val, ok := kv.store[op.Key]
	if ok {
		d.value = val
	} else {
		d.err = ErrNoKey
	}
	// kv.Debug("%v %v", op, kv.migratingDebugStr())
}

func (kv *ShardKV) applyPutAppend(op *Op, d *OpDone) {
	if op.SeqNum <= kv.dedup[op.ClientId] {
		return
	}
	if d.err = kv.checkOp(op); d.err != OK {
		// kv.DebugL("reject %v %v", op, d.err)
		return
	}

	if op.Type == PUT {
		kv.store[op.Key] = op.Value.(string)
	} else if op.Type == APPEND {
		kv.store[op.Key] += op.Value.(string)
	}
	kv.dedup[op.ClientId] = op.SeqNum
	// kv.Debug("%v %v", op, kv.migratingDebugStr())
}

func (kv *ShardKV) checkOp(op *Op) Err {
	if op.ConfigNum != kv.config.Num {
		return ErrWrongConfigNum
	}
	shard := key2shard(op.Key)
	if kv.config.Shards[shard] != kv.gid {
		return ErrWrongGroup
	}
	if kv.shardState[shard] != ShardServing {
		return ErrShardMigrating
	}
	return OK
}

func (kv *ShardKV) trySnapshot(lastIncludedIndex int) {
	if kv.maxraftstate == -1 ||
		kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.store); err != nil {
		log.Fatalf("s%d encode error: %v", kv.me, err)
	}
	if err := e.Encode(kv.config); err != nil {
		log.Fatalf("s%d encode error: %v", kv.me, err)
	}
	if err := e.Encode(kv.lastConfig); err != nil {
		log.Fatalf("s%d encode error: %v", kv.me, err)
	}
	if err := e.Encode(kv.shardState); err != nil {
		log.Fatalf("s%d encode error: %v", kv.me, err)
	}
	if err := e.Encode(kv.dedup); err != nil {
		log.Fatalf("s%d encode error: %v", kv.me, err)
	}
	snapshot := w.Bytes()

	kv.rf.Snapshot(lastIncludedIndex, snapshot)
	// kv.Debug("snapshot log{%d} %v", lastIncludedIndex, kv.migratingDebugStr())
}

func (kv *ShardKV) loadSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	var shardState [shardctrler.NShards]ShardState
	var dedup map[int]int
	if err := d.Decode(&store); err != nil {
		log.Fatalf("s%d decode error: %v", kv.me, err)
	}
	if err := d.Decode(&config); err != nil {
		log.Fatalf("s%d decode error: %v", kv.me, err)
	}
	if err := d.Decode(&lastConfig); err != nil {
		log.Fatalf("s%d decode error: %v", kv.me, err)
	}
	if err := d.Decode(&shardState); err != nil {
		log.Fatalf("s%d decode error: %v", kv.me, err)
	}
	if err := d.Decode(&dedup); err != nil {
		log.Fatalf("s%d decode error: %v", kv.me, err)
	}
	kv.store = store
	kv.config = config
	kv.lastConfig = lastConfig
	kv.shardState = shardState
	kv.dedup = dedup
}

func (kv *ShardKV) configLoop() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			nextNum := kv.config.Num + 1
			kv.mu.Unlock()

			nextConfig := kv.cck.Query(nextNum)
			if nextConfig.Num > 0 {
				kv.awaitOp(Op{Type: CONFIG, Value: nextConfig})
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) isMigrating() bool {
	for _, state := range kv.shardState {
		if state != ShardServing {
			return true
		}
	}
	return false
}

func (kv *ShardKV) applyConfig(op *Op) {
	if kv.isMigrating() {
		return
	}
	config := op.Value.(shardctrler.Config)
	if config.Num == kv.config.Num+1 {
		kv.lastConfig = kv.config
		kv.config = config
		if kv.lastConfig.Num > 0 {
			kv.setShardStates()
		}
		kv.Debug("%v %v", op, kv.migratingDebugStr())
	}
}

func (kv *ShardKV) setShardStates() {
	needPush := false
	for i := 0; i < shardctrler.NShards; i++ {
		from, to := kv.lastConfig.Shards[i], kv.config.Shards[i]
		if from == to {
			continue
		}
		if from == kv.gid {
			kv.shardState[i] = ShardPushing
			needPush = true
		} else if to == kv.gid {
			kv.shardState[i] = ShardPulling
		}
	}
	if needPush {
		kv.leaderRun(kv.pushShards)
	}
}

func (kv *ShardKV) leaderRun(f func()) {
	retry := 3
	for !kv.isLeader() && retry > 0 {
		time.Sleep(100 * time.Millisecond)
		retry--
	}
	f()
}

func (kv *ShardKV) pushShards() {
	gid2Data := make(map[int]map[string]string)
	gid2Ids := make(map[int][]int)
	for id, state := range kv.shardState {
		if state == ShardPushing {
			gid := kv.config.Shards[id]
			gid2Data[gid] = merge(gid2Data[gid], kv.getShardData(id))
			gid2Ids[gid] = append(gid2Ids[gid], id)
		}
	}
	for gid, data := range gid2Data {
		shard := Shard{
			Data:      data,
			Ids:       gid2Ids[gid],
			Gid:       kv.gid,
			ConfigNum: kv.config.Num,
			Servers:   kv.lastConfig.Groups[kv.gid],
			Dedup:     cloneMap(kv.dedup),
		}
		go kv.push(shard, kv.config.Groups[gid])
	}
}

func (kv *ShardKV) getShardData(shard int) map[string]string {
	ret := make(map[string]string)
	for k, v := range kv.store {
		if key2shard(k) == shard {
			ret[k] = v
		}
	}
	return ret
}

func merge(kvs ...map[string]string) map[string]string {
	ret := make(map[string]string)
	for _, kv := range kvs {
		for k, v := range kv {
			ret[k] = v
		}
	}
	return ret
}

func cloneMap(m map[int]int) map[int]int {
	ret := make(map[int]int)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func (kv *ShardKV) push(shard Shard, servers []string) {
	args := &MigrateArgs{Shard: shard}
	for {
		// try each server for the shard.
		for _, server := range servers {
			reply := &MigrateReply{}
			ok := kv.sendPull(server, args, reply)
			if ok && reply.Err == OK {
				return
			}
			// ... not ok, or ErrWrongLeader, ErrShardMigrating
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendPull(server string, args *MigrateArgs, reply *MigrateReply) bool {
	return kv.make_end(server).Call("ShardKV.Pull", args, reply)
}

func (kv *ShardKV) Pull(args *MigrateArgs, reply *MigrateReply) {
	_, reply.Err = kv.awaitOp(Op{Type: PULL, Value: args.Shard})
}

func (kv *ShardKV) applyPull(op *Op, d *OpDone) {
	shard := op.Value.(Shard)
	if shard.ConfigNum <= kv.dedup[DedupBasePull+shard.Gid] { // 已收到过分片
		// kv.Debug("applyPull return early: recieving v%d", shard.ConfigNum)
		return
	}
	if shard.ConfigNum > kv.config.Num { // 配置版本还没追上来，让clerk重试
		d.err = ErrShardMigrating
		// kv.Debug("applyPull reject %v: recieving v%d", d.err, shard.ConfigNum)
		return
	}

	for k, v := range shard.Data {
		kv.store[k] = v
	}
	for _, id := range shard.Ids {
		kv.shardState[id] = ShardServing
	}
	for k, v := range shard.Dedup {
		if k > DedupBaseClient && v > kv.dedup[k] {
			kv.dedup[k] = v
		}
	}
	kv.dedup[DedupBasePull+shard.Gid] = shard.ConfigNum
	kv.Debug("%v %v", op, kv.migratingDebugStr())

	kv.leaderRun(func() {
		keys := make([]string, len(shard.Data))
		for k := range shard.Data {
			keys = append(keys, k)
		}
		args := &DoneMigrateArgs{Ids: shard.Ids, Keys: keys, Gid: kv.gid, ConfigNum: kv.config.Num}
		go kv.donePull(args, shard.Servers)
	})
}

func (kv *ShardKV) donePull(args *DoneMigrateArgs, servers []string) {
	for {
		// try each server for the shard.
		for _, server := range servers {
			reply := &DoneMigrateReply{}
			ok := kv.sendDonePush(server, args, reply)
			if ok && reply.Err == OK {
				return
			}
			// ... not ok, or ErrWrongLeader, ErrShardMigrating
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendDonePush(server string, args *DoneMigrateArgs, reply *DoneMigrateReply) bool {
	return kv.make_end(server).Call("ShardKV.DonePush", args, reply)
}

func (kv *ShardKV) DonePush(args *DoneMigrateArgs, reply *DoneMigrateReply) {
	_, reply.Err = kv.awaitOp(Op{Type: DONEPUSH, Value: *args})
}

func (kv *ShardKV) applyDonePush(op *Op, d *OpDone) {
	args := op.Value.(DoneMigrateArgs)
	if args.ConfigNum <= kv.dedup[DedupBaseDonePush+args.Gid] { // 已收到过通知
		// kv.Debug("applyDonePush return early: recieving v%d", args.ConfigNum)
		return
	}
	for _, k := range args.Keys {
		delete(kv.store, k)
	}
	for _, id := range args.Ids {
		kv.shardState[id] = ShardServing
	}
	kv.dedup[DedupBaseDonePush+args.Gid] = args.ConfigNum
	kv.Debug("%v %v", op, kv.migratingDebugStr())
}
