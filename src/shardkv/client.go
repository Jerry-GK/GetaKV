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
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				args := GetArgs{key, ck.clientId, ck.nextMsgId, shard}
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// labutil.PrintMessage("Get: key = " + key + ", value = " + reply.Value + ", shard = " + strconv.Itoa(shard) + ", gid = " + strconv.Itoa(gid))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					time.Sleep(WaitForConfigConsistentTimeOut)
					break
				}
				if !ok || ok && (reply.Err == ErrWrongLeader) {
					// try next leader
					ck.leaderId = (ck.leaderId + 1) % len(servers)
					time.Sleep(TryNextGroupServerInterval)
					continue
				}
				if ok && (reply.Err == ErrTimeout) {
					time.Sleep(TryNextGroupServerInterval)
					continue
				}
			}
		}
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.nextMsgId = ck.getNextMsgId()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				args := PutAppendArgs{key, value, op, ck.clientId, ck.nextMsgId, shard}
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					// labutil.PrintMessage(args.Op + ": key = " + key + ", value = " + value + ", shard = " + strconv.Itoa(shard) + ", gid = " + strconv.Itoa(gid))
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					time.Sleep(WaitForConfigConsistentTimeOut)
					break
				}
				if !ok || ok && (reply.Err == ErrWrongLeader) {
					// try next leader
					ck.leaderId = (ck.leaderId + 1) % len(servers)
					time.Sleep(TryNextGroupServerInterval)
					continue
				}
				if ok && (reply.Err == ErrTimeout) {
					time.Sleep(TryNextGroupServerInterval)
					continue
				}
			}
		}
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
