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

const (
	WaitOpTimeOut         = time.Millisecond * 500
	TryNextLeaderInterval = time.Millisecond * 20
	ReconfigTimeOut       = time.Millisecond * 100
	WaitMigrateTimeOut    = time.Millisecond * 100
)

const (
	GET          = "Get"
	PUT          = "Put"
	APPEND       = "Append"
	MIGRATE      = "Migrate"
	UPDATECONFIG = "UpdateConfig"
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

	// for Migrate
	ShardKvData map[string]string
	ConfigNum   int
	OldGid      int

	// for UpdateConfig
	Config shardmaster.Config
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
	nextOpId          TypeOpId
	kvData            map[string]string
	stopCh            chan struct{}
	outPutCh          map[TypeOpId]chan ExeResult
	dead              int32                       // set by Kill()
	lastApplyMsgId    map[TypeClientId]ClerkMsgId //avoid duplicate apply
	mck               *shardmaster.Clerk
	config            shardmaster.Config
	receivedConfigNum map[int]int //record the configNum received from each group

	clientId  TypeClientId
	nextMsgId ClerkMsgId

	checkReconfigTimer *time.Timer
}

func (kv *ShardKV) getNextOpId() TypeOpId {
	//return kv.nextOpId + 1
	return TypeOpId(nrand()) //assume to be unique
}

func (kv *ShardKV) lock() {
	// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: lock")
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: unlock")
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.checkReconfig()

	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
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
	kv.checkReconfig()

	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
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

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:      MIGRATE,
		ClientId:    args.ClientId,
		MsgId:       args.MsgId,
		ServerId:    kv.me,
		OpId:        kv.nextOpId,
		ShardKvData: args.ShardKvData,
		ConfigNum:   args.ConfigNum,
		OldGid:      args.OldGid,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
}

func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs, reply *UpdateConfigReply) {
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   UPDATECONFIG,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
		Config:   args.Config,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
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
			case MIGRATE:
				if !isApplied {
					// issue: do we need to check kv.config.Num <= op.ConfigNum?
					for key, value := range op.ShardKvData {
						kv.kvData[key] = value
					}
					kv.receivedConfigNum[op.OldGid] = op.ConfigNum
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case UPDATECONFIG:
				if !isApplied {
					if kv.config.Num < op.Config.Num {
						kv.config = op.Config
					}
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
	e.Encode(kv.config)
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
	var config shardmaster.Config
	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplyMsgId) != nil ||
		d.Decode(&config) != nil {
		labutil.PrintException("KVServer[" + fmt.Sprint(kv.me) + "]: readSnapshot failed while decoding!")
		labutil.PanicSystem()
	} else {
		kv.kvData = kvData
		kv.lastApplyMsgId = lastApplyMsgId
		kv.config = config
		// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: readSnapshot, config = " + fmt.Sprint(kv.config.Num))
	}
}

func getShardListOf(config shardmaster.Config, gid int) []int {
	shards := make([]int, 0)
	for shard, group := range config.Shards {
		if group == gid {
			shards = append(shards, shard)
		}
	}
	return shards
}

func (ck *ShardKV) getNextMsgId() ClerkMsgId {
	return ck.nextMsgId + 1
}

func (kv *ShardKV) checkReconfig() {
	// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: checkReconfig")
	kv.lock()

	// only leader check reconfig
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.unlock()
		return
	}

	curConfig := kv.mck.Query(-1)
	if kv.config.Num < curConfig.Num {
		// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: Reconfig, curConfig = " + fmt.Sprint(curConfig.Num) + ", kv.config = " + fmt.Sprint(kv.config.Num))
		oldShardList := getShardListOf(kv.config, kv.gid)
		newShardList := getShardListOf(curConfig, kv.gid)
		// get deletedShards and addedShards
		migratedShards := make([]int, 0)
		for _, shard := range oldShardList {
			found := false
			for _, newShard := range newShardList {
				if shard == newShard {
					found = true
					break
				}
			}
			if !found {
				migratedShards = append(migratedShards, shard)
			}
		}

		// update config
		oldConfig := kv.config
		kv.nextMsgId = kv.getNextMsgId()
		args := UpdateConfigArgs{Config: curConfig, ClientId: kv.clientId, MsgId: kv.nextMsgId}
		reply := UpdateConfigReply{}
		kv.unlock()
		kv.UpdateConfig(&args, &reply)

		// migrate migratedShards to other groups
		if len(migratedShards) > 0 {
			for _, shard := range migratedShards {
				// labutil.PrintMessage("Server[" + fmt.Sprint(kv.me) + "]: Migrate shard = " + fmt.Sprint(shard) + " from gid = " + fmt.Sprint(oldConfig.Shards[shard]) + " to gid = " + fmt.Sprint(curConfig.Shards[shard]))
				kv.callMigrateShard(shard, kv.config.Shards[shard], oldConfig.Shards[shard])
			}
		}

		// wait until all shards from other group are migrated
		// receiveGroupList contains groups which have shards that was not belonged to itself
		receiveGroupList := make([]int, 0)
		addedMap := make(map[int]bool)
		for _, shard := range newShardList {
			oldGid := oldConfig.Shards[shard]
			if oldGid != 0 && oldGid != kv.gid && !addedMap[oldGid] {
				receiveGroupList = append(receiveGroupList, oldGid)
				addedMap[oldGid] = true
			}
		}

		for !kv.killed() {
			if !kv.receivedFromAllGroups(receiveGroupList) {
				time.Sleep(WaitMigrateTimeOut)
			} else {
				break
			}
		}
	} else {
		kv.unlock()
	}
}

func (kv *ShardKV) receivedFromAllGroups(groupList []int) bool {
	kv.lock()
	defer kv.unlock()
	for _, gid := range groupList {
		if configNum, ok := kv.receivedConfigNum[gid]; !ok || configNum < kv.config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) callMigrateShard(shard int, gid int, oldGid int) {
	// get shard data
	kv.lock()
	shardKvData := make(map[string]string)
	for key, value := range kv.kvData {
		if key2shard(key) == shard {
			shardKvData[key] = value
		}
	}

	// send data to new group
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := MigrateArgs{ShardKvData: shardKvData, ConfigNum: kv.config.Num, OldGid: oldGid, ClientId: kv.clientId, MsgId: kv.nextMsgId}
	for {
		for _, server := range kv.config.Groups[gid] {
			srv := kv.make_end(server)
			var reply MigrateReply
			ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
			if ok && reply.Err == OK {
				return
			} else if ok && reply.Err == ErrOutdatedConfig {
				return
			} else {
				time.Sleep(TryNextLeaderInterval)
			}
		}
	}
}

func (kv *ShardKV) periodicCheckReconfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.checkReconfigTimer.C:
			kv.checkReconfig()
			kv.checkReconfigTimer.Reset(ReconfigTimeOut)
		}
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
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)
	kv.receivedConfigNum = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.outPutCh = make(map[TypeOpId]chan ExeResult)

	kv.kvData = make(map[string]string)
	kv.lastApplyMsgId = make(map[TypeClientId]ClerkMsgId)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.clientId = TypeClientId(nrand()) //assume no duplicate
	kv.nextMsgId = 0

	kv.checkReconfigTimer = time.NewTimer(0)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.waitApply()

	go kv.periodicCheckReconfig()

	return kv
}
