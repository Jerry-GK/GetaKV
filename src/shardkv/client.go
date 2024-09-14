package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
	"../shardmaster"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  TypeClientId
	nextMsgId ClerkMsgId
}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = TypeClientId(nrand()) //assume no duplicate
	ck.nextMsgId = 0
	return ck
}

func (ck *Clerk) getNextMsgId() ClerkMsgId {
	return ck.nextMsgId + 1
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.nextMsgId = ck.getNextMsgId()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if ok {
			srv := ck.make_end(servers[ck.leaderId])
			args := GetArgs{key, ck.clientId, ck.nextMsgId, shard}
			var reply GetReply
			ok := srv.Call("ShardKV.Get", &args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % len(servers)
				time.Sleep(TryNextGroupServerInterval)
				continue
			}

			switch reply.Err {
			case OK, ErrNoKey:
				// labutil.PrintMessage("GET" + ": key = " + key + ", value = " + reply.Value + ", shard = " + strconv.Itoa(shard) + ", gid = " + strconv.Itoa(gid))
				return reply.Value
			case ErrWrongGroup:
				time.Sleep(WaitForConfigConsistentTimeOut)
				ck.config = ck.sm.Query(-1)
				continue
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(servers)
				time.Sleep(TryNextGroupServerInterval)
				continue
			case ErrTimeout:
				time.Sleep(TryNextGroupServerInterval)
				continue
			}
		} else {
			ck.config = ck.sm.Query(-1)
		}
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.nextMsgId = ck.getNextMsgId()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if ok {
			srv := ck.make_end(servers[ck.leaderId])
			args := PutAppendArgs{key, value, op, ck.clientId, ck.nextMsgId, shard}
			var reply PutAppendReply
			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
			if !ok {
				ck.leaderId = (ck.leaderId + 1) % len(servers)
				time.Sleep(TryNextGroupServerInterval)
				continue
			}

			switch reply.Err {
			case OK:
				// labutil.PrintMessage(args.Op + ": key = " + key + ", value = " + value + ", shard = " + strconv.Itoa(shard) + ", gid = " + strconv.Itoa(gid))
				return
			case ErrWrongGroup:
				time.Sleep(WaitForConfigConsistentTimeOut)
				ck.config = ck.sm.Query(-1)
				continue
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(servers)
				time.Sleep(TryNextGroupServerInterval)
				continue
			case ErrTimeout:
				time.Sleep(TryNextGroupServerInterval)
				continue
			}
		} else {
			ck.config = ck.sm.Query(-1)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
