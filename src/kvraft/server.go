package kvraft

import (
	"bytes"
	"fmt"

	//"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"

	"strconv"

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

	// Your definitions here.
	nextOpId        TypeOpId
	kvData         map[string]string
	lastApplyMsgId map[TypeClientId]ClerkMsgId //avoid duplicate apply

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// // issue: does read-only operation(like GET) need to go through Raft module?
	// // ans: Yes. To ensure consistency and order of operations?
	// _, isLeader := kv.rf.GetState()
	// reply.Err = OK
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }

	// if kv.kvData[args.Key] == "" {
	// 	//println("No key, msgId = " + fmt.Sprint(op.MsgId))
	// 	reply.Err = ErrNoKey
	// }
	// reply.Value = kv.kvData[args.Key]

	kv.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.UnLock()
		return
	}

	kv.nextOpId = kv.GetNextOpId()
	kv.SaveSnapshot()
	op := Op{
		Method:   GET,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
	}
	kv.UnLock()

	//println("serverID = " + fmt.Sprint(kv.me) + ", GET, key = " + op.Key + ", value = " + op.Value + ", msgId = " + fmt.Sprint(op.MsgId) + ", opId = " + fmt.Sprint(op.OpId) + ", clientId = " + fmt.Sprint(op.ClientId))
	res := kv.WaitOp(op)

	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) UnLock() {
	kv.mu.Unlock()
}

//must have outer lock!
func (kv *KVServer) GetNextOpId() TypeOpId {
	return  TypeOpId(nrand())
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.UnLock()
		return
	}

	kv.nextOpId = kv.GetNextOpId()
	op := Op{
		Method:   Method(args.Op),
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId, //assume to be unique
	}
	kv.UnLock()

	//println("serverID = " + fmt.Sprint(kv.me) + ", PUT, key = " + op.Key + ", value = " + op.Value + ", msgId = " + fmt.Sprint(op.MsgId) + ", opId = " + fmt.Sprint(op.OpId) + ", clientId = " + fmt.Sprint(op.ClientId))
	res := kv.WaitOp(op)

	reply.Err = res.Err
	//PutAppend does not return value

	if reply.Err == OK {
		//println("serverID = " + fmt.Sprint(kv.me) + ", PUT " + op.Value + " OK")
	}
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
	kv.Lock()
	defer kv.UnLock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	kv.Lock()
	defer kv.UnLock()
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//must have outer lock!
func (kv *KVServer) DeleteOutputCh(opId TypeOpId) {
	delete(kv.outPutCh, opId)
}

func (kv *KVServer) WaitOp(op Op) (res ExeResult) {
	res.Value = ""
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	waitOpTimer := time.NewTimer(WaitOpTimeOut)

	ch := make(chan ExeResult, 1)
	kv.Lock()
	kv.outPutCh[op.OpId] = ch
	kv.UnLock()
	for {
		select {
		case res_ := <-ch:
			//println("serverID = " + fmt.Sprint(kv.me) + ", ch rec value = " + op.Value)
			res.Err = res_.Err
			res.Value = res_.Value
			kv.Lock()
			kv.DeleteOutputCh(op.OpId)
			kv.UnLock()
			return
		case <-waitOpTimer.C:
			res.Err = ErrTimeout
			kv.Lock()
			kv.DeleteOutputCh(op.OpId)
			kv.UnLock()
			return
		}
	}
}

func (kv *KVServer) WaitApply() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				labutil.PrintException("Invalid Command!")
				labutil.PanicSystem()
			}

			op := msg.Command.(Op)
			ExeResult := ExeResult{Err: OK, Value: ""}

			kv.Lock()
			lastMsgId, ok := kv.lastApplyMsgId[op.ClientId]
			isApplied := ok && lastMsgId == op.MsgId
			kv.UnLock()

			//issue: is lastMsgId > op.MsgId possible?
			if lastMsgId > op.MsgId {
				labutil.PrintException("Bigger msgId!, lastMsgId = " + fmt.Sprint(lastMsgId) + ", op.MsgId = " + fmt.Sprint(op.MsgId))
				labutil.PanicSystem()
			}
			// _, isLeader := kv.rf.GetState()
			// if ok && lastMsgId < op.MsgId-1 && isLeader {
			// 	println("op.msgId = " + fmt.Sprint(op.MsgId))
			// 	println("lastApplyMsgId = " + fmt.Sprint(lastMsgId))
			// 	labutil.PrintException("Wrong msgId!")
			// 	labutil.PanicSystem()
			// }

			//real apply
			switch op.Method {
			case GET:
				// No data modification
				kv.Lock()
				if kv.kvData[op.Key] == "" {
					//println("No key, msgId = " + fmt.Sprint(op.MsgId))
					ExeResult.Err = ErrNoKey
				}
				ExeResult.Value = kv.kvData[op.Key]
				if !isApplied {
					//issue: is it neccessary to update lastApplyMsgId for GET?
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
				kv.UnLock()
				//println("Val = " + ExeResult.Value)
				//println("msgId = " + fmt.Sprint(op.MsgId))
			case PUT:
				kv.Lock()
				if !isApplied {
					kv.kvData[op.Key] = op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
					//println("serverID = " + fmt.Sprint(kv.me) + ", Apply Put: Key = " + op.Key + ", Value = " + op.Value + ", msgId = " + fmt.Sprint(op.MsgId))
				}
				kv.UnLock()
			case APPEND:
				kv.Lock()
				if !isApplied {
					kv.kvData[op.Key] += op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
				kv.UnLock()
			default:
				labutil.PrintException("Unknown Method")
				labutil.PanicSystem()
			}

			kv.Lock()
			kv.SaveSnapshot()
			ch, ok := kv.outPutCh[op.OpId]
			if ok && op.ServerId == kv.me {
				//println("serverID = " + fmt.Sprint(kv.me) + ", ch <- ExeResult, Value = " + op.Value + ", Method = " + fmt.Sprint(op.Method) + ", msgId = " + fmt.Sprint(op.MsgId) + ", opId = " + fmt.Sprint(op.OpId) + ", clientId = " + fmt.Sprint(op.ClientId))
				ch <- ExeResult
			}
			kv.UnLock()
		}
	}
}

//must have outer lock!
func (kv *KVServer) SaveSnapshot() {
	//rfData := kv.rf.GetPersistData()
	//kvData := kv.GetSnapshotData()
	//rfData = append(rfData, kvData...)
	//kv.rf.SavePersistAndSnapshot(rfData, kvData)
}

//must have outer lock!
func (kv *KVServer) GetSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.nextOpId)
	e.Encode(kv.kvData)
	e.Encode(kv.lastApplyMsgId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var nextOpId TypeOpId
	var kvData map[string]string
	var lastApplyMsgId map[TypeClientId]ClerkMsgId
	if d.Decode(&nextOpId) != nil ||
		d.Decode(&kvData) != nil ||
		d.Decode(&lastApplyMsgId) != nil {
		labutil.PrintException("KVServer[" + strconv.Itoa(kv.me) + "]: readSnapshot failed while decoding!")
		labutil.PanicSystem()
	} else {
		kv.nextOpId = nextOpId
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

	//kv.ReadSnapshot(persister.ReadSnapshot())

	kv.SaveSnapshot()

	go kv.WaitApply()

	return kv
}
