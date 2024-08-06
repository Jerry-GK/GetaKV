package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
	"../labutil"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	clientId  TypeClientId
	nextMsgId ClerkMsgId
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
	if len(servers) == 0 {
		labutil.PrintException("MakeClerk: No servers")
		labutil.PanicSystem()
		return nil
	}
	ck.leaderId = 0
	ck.clientId = TypeClientId(nrand()) //assume no duplicate
	ck.nextMsgId = 0
	return ck
}

func (ck *Clerk) getNextMsgId() ClerkMsgId {
	return ck.nextMsgId + 1
}

func (ck *Clerk) Query(num int) Config {
	ck.nextMsgId = ck.getNextMsgId()
	args := &QueryArgs{num, ck.clientId, ck.nextMsgId}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.nextMsgId = ck.getNextMsgId()
	args := &JoinArgs{servers, ck.clientId, ck.nextMsgId}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.nextMsgId = ck.getNextMsgId()
	args := &LeaveArgs{gids, ck.clientId, ck.nextMsgId}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.nextMsgId = ck.getNextMsgId()
	args := &MoveArgs{shard, gid, ck.clientId, ck.nextMsgId}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
