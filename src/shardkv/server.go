package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../labutil"
	"../raft"
	"../shardmaster"
)

const WaitOpTimeOut = time.Millisecond * 500

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type ExeResult struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Method Method
	Key    string
	Value  string

	ClientId TypeClientId
	MsgId    ClerkMsgId

	// ServerId + OpId is unique for each op
	ServerId int
	OpId     TypeOpId
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	nextOpId       TypeOpId
	kvData         map[string]string
	stopCh         chan struct{}
	outPutCh       map[TypeOpId]chan ExeResult
	dead           int32                       // set by Kill()
	lastApplyMsgId map[TypeClientId]ClerkMsgId //avoid duplicate appl
	mck            *shardmaster.Clerk
}

func (kv *ShardKV) getNextOpId() TypeOpId {
	//return kv.nextOpId + 1
	return TypeOpId(nrand()) //assume to be unique
}

func (kv *ShardKV) lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   GET,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   Method(args.Op),
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId, //assume to be unique
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
	//PutAppend does not return value
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.lock()
	defer kv.unlock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.saveSnapshot(0) //save before quit
	kv.rf.Kill()
	close(kv.stopCh)
}

func (kv *ShardKV) killed() bool {
	kv.lock()
	defer kv.unlock()
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) deleteOutputCh(opId TypeOpId) {
	delete(kv.outPutCh, opId)
}

func (kv *ShardKV) waitOp(op Op) (res ExeResult) {
	res.Value = ""
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	waitOpTimer := time.NewTimer(WaitOpTimeOut)

	ch := make(chan ExeResult, 1) //ch will be deleted if it receives the result from Raft module as a leader, or timeout

	kv.lock()
	kv.outPutCh[op.OpId] = ch
	kv.unlock()

	for {
		select {
		case res_ := <-ch:
			res.Err = res_.Err
			res.Value = res_.Value
			kv.lock()
			kv.deleteOutputCh(op.OpId)
			kv.unlock()
			return
		case <-waitOpTimer.C:
			res.Err = ErrTimeout
			kv.lock()
			kv.deleteOutputCh(op.OpId)
			kv.unlock()
			return
		}
	}
}

func (kv *ShardKV) waitApply() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.lock()
				kv.readSnapshot(kv.persister.ReadSnapshot()) //read snapshot if left behind
				kv.unlock()
				continue
			}

			op := msg.Command.(Op)
			ExeResult := ExeResult{Err: OK, Value: ""}

			kv.lock()

			lastMsgId, ok := kv.lastApplyMsgId[op.ClientId]
			isApplied := ok && lastMsgId == op.MsgId

			//issue: is lastMsgId > op.MsgId possible?
			//ans: maybe possible, if the client re-send the request?
			// if lastMsgId > op.MsgId {
			// 	labutil.PrintException("Bigger msgId!, lastMsgId = " + fmt.Sprint(lastMsgId) + ", op.MsgId = " + fmt.Sprint(op.MsgId))
			// 	labutil.PanicSystem()
			// }

			//real apply
			switch op.Method {
			case GET:
				// No data modification
				if kv.kvData[op.Key] == "" {
					ExeResult.Err = ErrNoKey
				}
				ExeResult.Value = kv.kvData[op.Key]
				if !isApplied {
					//issue: is it neccessary to update lastApplyMsgId for GET?
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case PUT:
				if !isApplied {
					kv.kvData[op.Key] = op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case APPEND:
				if !isApplied {
					kv.kvData[op.Key] += op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			default:
				labutil.PrintException("Unknown Method")
				labutil.PanicSystem()
			}

			kv.saveSnapshot(msg.CommandIndex) //must persist the apply result before return
			ch, ok := kv.outPutCh[op.OpId]
			if ok && op.ServerId == kv.me {
				ch <- ExeResult
			}
			kv.unlock()
		}
	}
}

// must have outer lock!
func (kv *ShardKV) saveSnapshot(index int) {
	//save snapshot only when raftstate size exceeds
	//Start(cmd) -> apply -> raftstate size grows -> (if exceeds) save snapshot
	if index == 0 || kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
		kvData := kv.getSnapshotData()
		//labutil.PrintDebug("Server[" + fmt.Sprint(kv.me) + "]: Saving snapshot, index = " + fmt.Sprint(index))
		kv.rf.SavePersistAndSnapshot(index, kvData)
	}
}

// must have outer lock!
func (kv *ShardKV) getSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvData)
	e.Encode(kv.lastApplyMsgId)
	data := w.Bytes()
	return data
}

// may be called by other modules
func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplyMsgId map[TypeClientId]ClerkMsgId
	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplyMsgId) != nil {
		labutil.PrintException("KVServer[" + fmt.Sprint(kv.me) + "]: readSnapshot failed while decoding!")
		labutil.PanicSystem()
	} else {
		kv.kvData = kvData
		kv.lastApplyMsgId = lastApplyMsgId
	}
}

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
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.nextOpId = 0

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.outPutCh = make(map[TypeOpId]chan ExeResult)

	kv.kvData = make(map[string]string)
	kv.lastApplyMsgId = make(map[TypeClientId]ClerkMsgId)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.waitApply()

	return kv
}
