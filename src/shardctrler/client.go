package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader int
	RequestId
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.lastLeader = int(nrand()) % len(ck.servers)
	ck.RequestId = RequestId{GetClientId(), 0}

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.SeqNum++
	args := QueryArgs{Num: num, RequestId: ck.RequestId}
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Query", &args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		// not ok, or WrongLeader
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.SeqNum++
	args := JoinArgs{Servers: servers, RequestId: ck.RequestId}
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Join", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		// not ok, or WrongLeader
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.SeqNum++
	args := LeaveArgs{GIDs: gids, RequestId: ck.RequestId}
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Leave", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.SeqNum++
	args := MoveArgs{Shard: shard, GID: gid, RequestId: ck.RequestId}
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
