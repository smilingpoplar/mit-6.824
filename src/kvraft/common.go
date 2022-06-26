package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestId struct {
	ClientId int
	SeqNum   int
}

func ClientIdGenerator() func() int {
	start := int(nrand()) % 1e4
	count := 0
	return func() (ret int) {
		ret = start + count
		count++
		return
	}
}

var GetClientId = ClientIdGenerator()
