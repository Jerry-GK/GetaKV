package kvraft

import (
	"bytes"
	"fmt"

	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"

	"../labutil"
	"../raft"
)

const Debug = 0
const WaitOpTimeOut = time.Millisecond * 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Method string

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

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	stopCh   chan struct{}
	outPutCh map[TypeOpId]chan ExeResult
	dead     int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister

	nextOpId TypeOpId
	//persistent
	kvData         map[string]string
	lastApplyMsgId map[TypeClientId]ClerkMsgId //avoid duplicate apply
}

func (kv *KVServer) GetRf() *raft.Raft {
	kv.lock()
	defer kv.unlock()
	return kv.rf
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// // issue: does read-only operation(like GET) need to go through Raft module?
	// // ans: Yes. To ensure consistency and order of operations?

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

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.mu.Unlock()
}

//must have outer lock!
func (kv *KVServer) getNextOpId() TypeOpId {
	//return kv.nextOpId + 1
	return TypeOpId(nrand()) //assume to be unique
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	kv.lock()
	defer kv.unlock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	kv.lock()
	defer kv.unlock()
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//must have outer lock!
func (kv *KVServer) deleteOutputCh(opId TypeOpId) {
	delete(kv.outPutCh, opId)
}

func (kv *KVServer) waitOp(op Op) (res ExeResult) {
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

func (kv *KVServer) waitApply() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.lock()
				kv.ReadSnapshot(kv.persister.ReadSnapshot()) //read snapshot if left behind
				kv.unlock()
				continue
			}

			op := msg.Command.(Op)
			ExeResult := ExeResult{Err: OK, Value: ""}

			kv.lock()
			lastMsgId, ok := kv.lastApplyMsgId[op.ClientId]
			isApplied := ok && lastMsgId == op.MsgId
			kv.unlock()

			//issue: is lastMsgId > op.MsgId possible?
			if lastMsgId > op.MsgId {
				labutil.PrintException("Bigger msgId!, lastMsgId = " + fmt.Sprint(lastMsgId) + ", op.MsgId = " + fmt.Sprint(op.MsgId))
				labutil.PanicSystem()
			}

			//real apply
			switch op.Method {
			case GET:
				// No data modification
				kv.lock()
				if kv.kvData[op.Key] == "" {
					ExeResult.Err = ErrNoKey
				}
				ExeResult.Value = kv.kvData[op.Key]
				if !isApplied {
					//issue: is it neccessary to update lastApplyMsgId for GET?
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
				kv.unlock()
			case PUT:
				kv.lock()
				if !isApplied {
					kv.kvData[op.Key] = op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
				kv.unlock()
			case APPEND:
				kv.lock()
				if !isApplied {
					kv.kvData[op.Key] += op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
				kv.unlock()
			default:
				labutil.PrintException("Unknown Method")
				labutil.PanicSystem()
			}

			kv.lock()
			kv.saveSnapshot(msg.CommandIndex)
			ch, ok := kv.outPutCh[op.OpId]
			if ok && op.ServerId == kv.me {
				ch <- ExeResult
			}
			kv.unlock()
		}
	}
}

//must have outer lock!
func (kv *KVServer) saveSnapshot(index int) {
	//save snapshot only when raftstate size exceeds
	//Start(cmd) -> apply -> raftstate size grows -> (if exceeds) save snapshot
	if kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
		kvData := kv.getSnapshotData()
		//labutil.PrintDebug("Server[" + fmt.Sprint(kv.me) + "]: Saving snapshot, index = " + fmt.Sprint(index))
		kv.rf.SavePersistAndSnapshot(index, kvData)
	}
}

//must have outer lock!
func (kv *KVServer) getSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvData)
	e.Encode(kv.lastApplyMsgId)
	data := w.Bytes()
	return data
}

//may be called by other modules
func (kv *KVServer) ReadSnapshot(data []byte) {
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.nextOpId = 0
	kv.persister = persister

	// You may need initialization code here.

	// applyCh is shared by server and its raft module
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.stopCh = make(chan struct{})

	kv.outPutCh = make(map[TypeOpId]chan ExeResult)

	kv.kvData = make(map[string]string)
	kv.lastApplyMsgId = make(map[TypeClientId]ClerkMsgId)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.saveSnapshot(0) //maybe unnecessary

	go kv.waitApply()

	return kv
}
