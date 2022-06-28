package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrWrongConfigNum = "ErrWrongConfigNum"
	ErrShardMigrating = "ErrShardMigrating"
)

type Err string

const (
	GET      = "Get"
	PUT      = "Put"
	APPEND   = "Append"
	CONFIG   = "Config"
	PULL     = "Pull"
	DONEPUSH = "DonePush"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ConfigNum int
	RequestId
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfigNum int
	RequestId
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Shard
}

type MigrateReply struct {
	Err Err
}

type DoneMigrateArgs struct {
	Ids       []int
	Keys      []string
	Gid       int
	ConfigNum int
}

type DoneMigrateReply struct {
	Err Err
}

type RequestId struct {
	ClientId int
	SeqNum   int
}

func ClientIdGenerator() func() int {
	start := DedupBaseClient + int(nrand())%1e4
	count := 0
	return func() (ret int) {
		ret = start + count
		count++
		return
	}
}

var GetClientId = ClientIdGenerator()
