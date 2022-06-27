package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opDoneCh map[int]chan OpDone // logIndex => opDoneCh
	opDoneMu sync.Mutex
	dedup    map[int]int // clientId => lastSeqNum
	dead     int32       // set by Kill()

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type string // Join/Leave/Move/Query
	Args interface{}
	RequestId
}

type OpDone struct {
	value interface{}
	RequestId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Type: JOIN, Args: *args, RequestId: args.RequestId}
	_, reply.WrongLeader = sc.awaitOp(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: LEAVE, Args: *args, RequestId: args.RequestId}
	_, reply.WrongLeader = sc.awaitOp(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: MOVE, Args: *args, RequestId: args.RequestId}
	_, reply.WrongLeader = sc.awaitOp(op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: QUERY, Args: *args, RequestId: args.RequestId}
	r, wrongLeader := sc.awaitOp(op)
	if wrongLeader {
		reply.WrongLeader = wrongLeader
		return
	}

	reply.Config = r.(Config)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opDoneCh = make(map[int]chan OpDone)
	sc.dedup = make(map[int]int)
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	go sc.applyLoop()

	return sc
}

func (sc *ShardCtrler) awaitOp(op Op) (interface{}, bool) {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return nil, true
	}

	ch := make(chan OpDone, 1) // 不阻塞
	sc.opDoneMu.Lock()
	sc.opDoneCh[index] = ch
	sc.opDoneMu.Unlock()
	defer sc.delOpDoneCh(index)

	select {
	case d := <-ch:
		if d.RequestId != op.RequestId {
			return nil, true
		}
		return d.value, false
	case <-time.After(time.Second): // 超时返回
		return nil, true
	}
}

func (sc *ShardCtrler) delOpDoneCh(index int) {
	sc.opDoneMu.Lock()
	delete(sc.opDoneCh, index)
	sc.opDoneMu.Unlock()
}

func (sc *ShardCtrler) applyLoop() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			d := OpDone{RequestId: op.RequestId}
			sc.mu.Lock()
			if op.Type == QUERY {
				sc.applyQuery(&op, &d)
			} else {
				if d.SeqNum > sc.dedup[d.ClientId] {
					config := sc.makeConfig()
					switch op.Type {
					case JOIN:
						sc.applyJoin(&op, &config)
					case LEAVE:
						sc.applyLeave(&op, &config)
					case MOVE:
						sc.applyMove(&op, &config)
					}
					sc.configs = append(sc.configs, config)
					sc.dedup[d.ClientId] = d.SeqNum
				}
			}
			sc.mu.Unlock()

			sc.opDoneMu.Lock()
			ch, ok := sc.opDoneCh[msg.CommandIndex]
			sc.opDoneMu.Unlock()
			if ok {
				ch <- d
			}
		}
	}
}

func (sc *ShardCtrler) applyQuery(op *Op, d *OpDone) {
	args := op.Args.(QueryArgs)
	cfgCount := len(sc.configs)
	if args.Num < 0 { // 查询最新
		d.value = sc.configs[cfgCount-1]
	} else if args.Num < cfgCount {
		d.value = sc.configs[args.Num]
	} else { // dummy配置
		d.value = sc.configs[0]
	}
}

func (sc *ShardCtrler) makeConfig() Config {
	prev := sc.configs[len(sc.configs)-1]
	next := Config{Num: prev.Num + 1, Shards: prev.Shards, Groups: make(map[int][]string)}
	for gid, servers := range prev.Groups { // gid -> servers[]
		next.Groups[gid] = append([]string{}, servers...)
	}
	return next
}

func (sc *ShardCtrler) applyJoin(op *Op, config *Config) {
	args := op.Args.(JoinArgs)
	for gid, servers := range args.Servers { // map: gid -> servers[]
		config.Groups[gid] = servers
	}
	sc.rebalance(config)
}

func (sc *ShardCtrler) applyLeave(op *Op, config *Config) {
	args := op.Args.(LeaveArgs)
	for _, gid := range args.GIDs { // array
		for shard, shardGid := range config.Shards { // array: shard -> gid
			if shardGid == gid {
				config.Shards[shard] = 0 // 返还到初始组0
			}
		}
		delete(config.Groups, gid)
	}
	sc.rebalance(config)
}

func (sc *ShardCtrler) applyMove(op *Op, config *Config) {
	args := op.Args.(MoveArgs)
	config.Shards[args.Shard] = args.GID
}

type Gid_ShardCount struct {
	gid        int
	shardCount int
}

func (sc *ShardCtrler) rebalance(config *Config) {
	if len(config.Groups) <= 0 {
		return
	}
	// 每组对应哪些分片，含gid=0存放所有未分配分片
	gidToShards := make(map[int][]int)      // map: gid -> shards
	for shard, gid := range config.Shards { // array: shard -> gid
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	// 对gid>0的{gid,shardCount}数组按shardCount从大到小排序
	arr := make([]Gid_ShardCount, 0)
	for gid := range config.Groups { // map: gid -> servers[]
		shardCount := len(gidToShards[gid])
		arr = append(arr, Gid_ShardCount{gid, shardCount})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].shardCount == arr[j].shardCount {
			return arr[i].gid < arr[j].gid
		}
		return arr[i].shardCount > arr[j].shardCount
	})
	// 前面remainder组分配average+1个分片，其他组分配average个分片
	groupCount := len(config.Groups)
	average, remainder := NShards/groupCount, NShards%groupCount
	moreMap := make(map[int]int) // gid -> 多余分片数
	for i, gs := range arr {
		if i < remainder {
			moreMap[gs.gid] = gs.shardCount - (average + 1)
		} else {
			moreMap[gs.gid] = gs.shardCount - average
		}
	}

	// 重新分配分片
	unalloc := gidToShards[0]
	for _, gs := range arr {
		if gid, more := gs.gid, moreMap[gs.gid]; more > 0 {
			unalloc = append(unalloc, gidToShards[gid][:more]...)
			gidToShards[gid] = gidToShards[gid][more:]
		}
	}
	for _, gs := range arr {
		if gid, more := gs.gid, moreMap[gs.gid]; more < 0 {
			gidToShards[gid] = append(gidToShards[gid], unalloc[:-more]...)
			unalloc = unalloc[-more:]
		}
	}
	// 根据gidToShards更新config.Shards
	for gid, shards := range gidToShards {
		if gid > 0 {
			for _, shard := range shards {
				config.Shards[shard] = gid
			}
		}
	}
}
