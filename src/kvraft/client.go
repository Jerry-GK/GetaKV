package kvraft

import (
	"crypto/rand"
	//"fmt"
	"math/big"

	//"strconv"
	"time"

	"../labrpc"
	"../labutil"
)

const (
	TryNextLeaderInterval = time.Millisecond * 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
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

func (ck *Clerk) GetNextMsgId() ClerkMsgId {
	return ck.nextMsgId + 1
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	ck.nextMsgId = ck.GetNextMsgId()
	args := GetArgs{key, ck.clientId, ck.nextMsgId}
	//args.msgId is the same for multiple RPC retries

	//labutil.PrintMessage("Clerk[" + fmt.Sprint(ck.clientId) + "] Get" + " <" + key + ">" + ", msgId = " + fmt.Sprint(ck.nextMsgId))

	for {
		reply := GetReply{}
		//labutil.PrintMessage("Try Leader = Server[" + fmt.Sprint(ck.leaderId) + "]")
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			//try next server to be leader
			time.Sleep(TryNextLeaderInterval)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			//println("Get Success")
			return reply.Value
		case ErrNoKey:
			//println("Get NoKey")
			return ""
		case ErrWrongLeader:
			//try next server to be leader
			time.Sleep(TryNextLeaderInterval)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeout:
			//println("Timeout")
			continue
		default:
			labutil.PrintException("Get: Unknown GetReply.Err")
			labutil.PanicSystem()
			return ""
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.nextMsgId = ck.GetNextMsgId()
	args := PutAppendArgs{key, value, op, ck.clientId, ck.nextMsgId}
	//args.msgId is the same for multiple RPC retries

	//labutil.PrintMessage("Clerk[" + fmt.Sprint(ck.clientId) + "] " + op + " <" + key + ", " + value + ">" + ", msgId = " + fmt.Sprint(ck.nextMsgId))

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			//try next server to be leader
			time.Sleep(TryNextLeaderInterval)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			//println("AP success")
			return
		case ErrNoKey:
			//PutAppend should not return ErrNoKey in reply
			labutil.PrintException("PutAppend return ErrNoKey in reply")
			labutil.PanicSystem()
			return
		case ErrWrongLeader:
			//try next server to be leader
			time.Sleep(TryNextLeaderInterval)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeout:
			//println("TimeoutAp")
			continue
		default:
			labutil.PrintException("Get: Unknown GetReply.Err")
			labutil.PanicSystem()
			return
		}
	}
	return //should never reach here
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
