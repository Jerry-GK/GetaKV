package shardkv

import (
	"bytes"
	"context"
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
	WaitOpTimeOut      = time.Millisecond * 500
	ReconfigTimeOut    = time.Millisecond * 100
	WaitMigrateTimeOut = time.Millisecond * 100
	QueryConfigTimeout = time.Millisecond * 2500
)

const (
	GET                  = "Get"
	PUT                  = "Put"
	APPEND               = "Append"
	MIGRATESHARDS        = "MigrateShards"
	UPDATECONFIG         = "UpdateConfig"
	GETCONFIG            = "GetConfig"
	GETSHARDSDATA        = "GetShardsData"
	GETMIGRATINGSARDS    = "GetMigratingShards"
	UPDATEMIGRATINGSARDS = "UpdateMigratingShards"
	GETRECEIVECONFIGNUM  = "GetReceiveConfigNum"
)

const delete_unused_shards = true

type ExeResult struct {
	Err              Err
	Value            string
	Config           shardmaster.Config
	ShardsKvData     map[string]string
	MigratingShards  []int
	ReceiveConfigNum map[int]int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Method Method

	DataShard int

	Key   string
	Value string

	ClientId TypeClientId
	MsgId    ClerkMsgId

	// ServerId + OpId is unique for each op
	ServerId int
	OpId     TypeOpId

	// for Migrate
	ShardsKvData map[string]string
	ConfigNum    int
	OldGid       int
	IsNewGroup   bool

	// for UpdateConfig
	Config       shardmaster.Config
	DeleteShards []int

	// for GetShardsData
	GetShards []int

	// for UpdateMigratingShards
	MigratingShards []int
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
	nextOpId         TypeOpId
	kvData           map[string]string
	stopCh           chan struct{}
	outPutCh         map[TypeOpId]chan ExeResult
	dead             int32                       // set by Kill()
	lastApplyMsgId   map[TypeClientId]ClerkMsgId //avoid duplicate apply
	mck              *shardmaster.Clerk
	config           shardmaster.Config
	receiveConfigNum map[int]int //record the configNum receive from each group

	migratingShards []int // shards that is being migrated in a config (reset empty after config completely updated)

	clientId  TypeClientId
	nextMsgId ClerkMsgId

	checkReconfigTimer *time.Timer

	randID int64 // for debug
}

func (kv *ShardKV) selfString() string {
	// Group[gid] - Server[me]
	s := "Group[" + fmt.Sprint(kv.gid) + "] - Server[" + fmt.Sprint(kv.me) + "] - ID[" + fmt.Sprint(kv.randID) + "]: "
	return s
}

func (kv *ShardKV) getNextOpId() TypeOpId {
	//return kv.nextOpId + 1
	return TypeOpId(nrand()) //assume to be unique
}

func (kv *ShardKV) lock() {
	// labutil.PrintMessage(kv.selfString() + "lock")
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	// labutil.PrintMessage(kv.selfString() + "unlock")
	kv.mu.Unlock()
}

// internal virtual client caller
func (kv *ShardKV) CallGetConfig() shardmaster.Config {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := GetConfigArgs{ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply GetConfigReply
		kv.GetConfig(&args, &reply)

		switch reply.Err {
		case OK:
			return reply.Config
		case ErrWrongLeader:
			return shardmaster.Config{} // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

// internal virtual client caller
func (kv *ShardKV) CallUpdateConfig(config shardmaster.Config, deleteShards []int) {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := UpdateConfigArgs{Config: config, DeleteShards: deleteShards, ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply UpdateConfigReply
		kv.UpdateConfig(&args, &reply)

		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			return // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

// internal virtual client caller
func (kv *ShardKV) CallGetMigratingShards() []int {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := GetMigratingShardsArgs{ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply GetMigratingShardsReply
		kv.GetMigratingShards(&args, &reply)

		switch reply.Err {
		case OK:
			return reply.MigratingShards
		case ErrWrongLeader:
			return []int{} // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

// internal virtual client caller
func (kv *ShardKV) CallUpdateMigratingShards(migratingShards []int) {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := UpdateMigratingShardsArgs{MigratingShards: migratingShards, ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply UpdateMigratingShardsReply
		// fmt.Println(kv.selfString() + "CallUpdateMigratingShards, migratingShards = " + fmt.Sprint(migratingShards))
		kv.UpdateMigratingShards(&args, &reply)

		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			return // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

func (kv *ShardKV) callGetReceiveConfigNum() map[int]int {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := GetReceiveConfigNumArgs{ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply GetReceiveConfigNumReply
		kv.GetReceiveConfigNum(&args, &reply)

		switch reply.Err {
		case OK:
			return reply.ReceiveConfigNum
		case ErrWrongLeader:
			return map[int]int{} // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

// internal virtual client caller
func (kv *ShardKV) CallGetShardsData(shards []int) map[string]string {
	kv.lock()
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()
	args := GetShardsDataArgs{Shards: shards, ClientId: kv.clientId, MsgId: kv.nextMsgId}

	for {
		var reply GetShardsDataReply
		kv.GetShardsData(&args, &reply)

		switch reply.Err {
		case OK:
			return reply.ShardsData
		case ErrWrongLeader:
			return map[string]string{} // return immediately if wrong leader for internal request (never here theoretically)
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval) // retry after timeout for internal request
			continue
		}
	}
}

// Shard KV RPCs
func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:       MIGRATESHARDS,
		ClientId:     args.ClientId,
		MsgId:        args.MsgId,
		ServerId:     kv.me,
		OpId:         kv.nextOpId,
		ShardsKvData: args.ShardsKvData,
		ConfigNum:    args.ConfigNum,
		OldGid:       args.FromGid,
		IsNewGroup:   args.IsNewGroup,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) GetConfig(args *GetConfigArgs, reply *GetConfigReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   GETCONFIG,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Config = res.Config
	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs, reply *UpdateConfigReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:       UPDATECONFIG,
		ClientId:     args.ClientId,
		MsgId:        args.MsgId,
		ServerId:     kv.me,
		OpId:         kv.nextOpId,
		Config:       args.Config,
		DeleteShards: args.DeleteShards,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) GetMigratingShards(args *GetMigratingShardsArgs, reply *GetMigratingShardsReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   GETMIGRATINGSARDS,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.MigratingShards = res.MigratingShards

	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) UpdateMigratingShards(args *UpdateMigratingShardsArgs, reply *UpdateMigratingShardsReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:          UPDATEMIGRATINGSARDS,
		ClientId:        args.ClientId,
		MsgId:           args.MsgId,
		ServerId:        kv.me,
		OpId:            kv.nextOpId,
		MigratingShards: args.MigratingShards,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) GetShardsData(args *GetShardsDataArgs, reply *GetShardsDataReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:    GETSHARDSDATA,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  kv.me,
		OpId:      kv.nextOpId,
		GetShards: args.Shards,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.ShardsData = res.ShardsKvData
	reply.Err = res.Err
}

// only for internal request
func (kv *ShardKV) GetReceiveConfigNum(args *GetReceiveConfigNumArgs, reply *GetReceiveConfigNumReply) {
	killed := kv.killed()
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   GETRECEIVECONFIGNUM,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		ServerId: kv.me,
		OpId:     kv.nextOpId,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.ReceiveConfigNum = res.ReceiveConfigNum
	reply.Err = res.Err
}

// outside client request
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	killed := kv.killed()
	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:    GET,
		DataShard: args.Shard,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  kv.me,
		OpId:      kv.nextOpId,
	}
	kv.unlock()

	res := kv.waitOp(op)

	reply.Err = res.Err
	reply.Value = res.Value
}

// outside client request
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	killed := kv.killed()
	//kv.checkReconfig()

	// Your code here.
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:    Method(args.Op),
		DataShard: args.Shard,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  kv.me,
		OpId:      kv.nextOpId, //assume to be unique
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
	// fmt.Println(kv.selfString()+"Killing server", ", lastApplyMsgId = ", kv.lastApplyMsgId)
	defer kv.unlock()
	atomic.StoreInt32(&kv.dead, 1)
	// kv.saveSnapshot(0) //save before quit
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
			res.Config = res_.Config
			res.ShardsKvData = res_.ShardsKvData
			res.MigratingShards = res_.MigratingShards
			res.ReceiveConfigNum = res_.ReceiveConfigNum
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
			isApplied := ok && lastMsgId >= op.MsgId

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
				if kv.config.Shards[op.DataShard] != kv.gid {
					ExeResult.Err = ErrWrongGroup
					break
				} else if kv.kvData[op.Key] == "" {
					ExeResult.Err = ErrNoKey // don't return
				}
				ExeResult.Value = kv.kvData[op.Key]
				if !isApplied {
					//issue: is it neccessary to update lastApplyMsgId for GET?
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case PUT:
				if !isApplied {
					migrating := false
					for _, shard := range kv.migratingShards {
						if shard == op.DataShard {
							migrating = true
						}
					}
					if kv.config.Shards[op.DataShard] != kv.gid || migrating {
						ExeResult.Err = ErrWrongGroup
						break
					}
					kv.kvData[op.Key] = op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case APPEND:
				if !isApplied {
					migrating := false
					for _, shard := range kv.migratingShards {
						if shard == op.DataShard {
							migrating = true
						}
					}
					if kv.config.Shards[op.DataShard] != kv.gid || migrating {
						ExeResult.Err = ErrWrongGroup
						break
					}
					kv.kvData[op.Key] += op.Value
					// fmt.Println(kv.selfString() + "Append, key = " + op.Key + ", value = " + op.Value)
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case MIGRATESHARDS:
				if !isApplied {
					configNum := kv.receiveConfigNum[op.OldGid]
					if configNum >= op.ConfigNum || kv.config.Num > op.ConfigNum-1 {
						// avoid duplicate migrate during restart (otherwise appended data during restart may be recovered!)
						ExeResult.Err = ErrAlreadyMigrated
					} else if kv.config.Num == op.ConfigNum-1 || op.IsNewGroup {
						// check kv.config.Num >= op.ConfigNum-1 to process reconfig one at a time, this condition is critical!
						// new group does not need to check configNum
						for key, value := range op.ShardsKvData {
							kv.kvData[key] = value
						}
						kv.receiveConfigNum[op.OldGid] = op.ConfigNum
						// fmt.Println(kv.selfString()+"Migrate success, kv.config.Num = "+fmt.Sprint(kv.config.Num)+", op.ConfigNum = "+fmt.Sprint(op.ConfigNum)+", oldGid = "+fmt.Sprint(op.OldGid)+", msgId = "+fmt.Sprint(op.MsgId), ", shardsData = "+fmt.Sprint(op.ShardsKvData))
					} else {
						ExeResult.Err = ErrConfigNotMatch
						// fmt.Println(kv.selfString()+"Migrate failed, kv.config.Num = "+fmt.Sprint(kv.config.Num)+", op.ConfigNum = "+fmt.Sprint(op.ConfigNum)+", oldGid = "+fmt.Sprint(op.OldGid)+", msgId = "+fmt.Sprint(op.MsgId), ", shardsData = "+fmt.Sprint(op.ShardsKvData))
						break // do not update lastApplyMsgId
					}
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case GETCONFIG:
				if !isApplied {
					// deep copy kv.config to ExeResult.Config
					ExeResult.Config = shardmaster.Config{}
					ExeResult.Config.Num = kv.config.Num
					ExeResult.Config.Shards = kv.config.Shards
					ExeResult.Config.Groups = make(map[int][]string)
					for gid, servers := range kv.config.Groups {
						ExeResult.Config.Groups[gid] = servers
					}
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case UPDATECONFIG:
				if !isApplied {
					if kv.config.Num < op.Config.Num {
						leftGroupList := make([]int, 0)
						for gid := range kv.config.Groups {
							found := false
							for oldGid := range op.Config.Groups {
								if gid == oldGid {
									found = true
									break
								}
							}
							if !found && gid != kv.gid {
								leftGroupList = append(leftGroupList, gid)
							}
						}

						// 1.reset receiveConfigNum of left groups
						for _, gid := range leftGroupList {
							delete(kv.receiveConfigNum, gid)
						}

						// 2.reset migrating shards empty
						kv.migratingShards = make([]int, 0)

						// 3.delete unused shards
						if delete_unused_shards {
							for key := range kv.kvData {
								shard := key2shard(key)
								for _, s := range op.DeleteShards {
									if s == shard {
										delete(kv.kvData, key)
										break
									}
								}
							}
						}

						// 4.update config
						kv.config = op.Config
					}
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case GETMIGRATINGSARDS:
				if !isApplied {
					// no need to deep copy an array
					ExeResult.MigratingShards = kv.migratingShards
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case UPDATEMIGRATINGSARDS:
				if !isApplied {
					kv.migratingShards = op.MigratingShards
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case GETSHARDSDATA:
				shardsKvData := make(map[string]string)
				if !isApplied {
					// deep copy the shards data
					for key, value := range kv.kvData {
						shard := key2shard(key)
						for _, s := range op.GetShards {
							if s == shard {
								shardsKvData[key] = value
								break
							}
						}
					}
					ExeResult.ShardsKvData = shardsKvData
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case GETRECEIVECONFIGNUM:
				if !isApplied {
					// deep copy kv.receiveConfigNum to ExeResult.ReceiveConfigNum
					ExeResult.ReceiveConfigNum = make(map[int]int)
					for gid, num := range kv.receiveConfigNum {
						ExeResult.ReceiveConfigNum[gid] = num
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
	//save snapshot only when raftstate size exceeds or forced(index=0)
	//Start(cmd) -> apply -> raftstate size grows -> (if exceeds) save snapshot
	if kv.maxraftstate != -1 && (index == 0 || kv.maxraftstate <= kv.persister.RaftStateSize()) {
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
	e.Encode(kv.receiveConfigNum)
	e.Encode(kv.migratingShards)
	e.Encode(kv.nextMsgId)
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
	var receiveConfigNum map[int]int
	var migratingShards []int
	var nextMsgId ClerkMsgId
	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplyMsgId) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&receiveConfigNum) != nil ||
		d.Decode(&migratingShards) != nil ||
		d.Decode(&nextMsgId) != nil {
		labutil.PrintException("KVServer[" + fmt.Sprint(kv.me) + "]: readSnapshot failed while decoding!")
		labutil.PanicSystem()
	} else {
		kv.kvData = kvData
		kv.lastApplyMsgId = lastApplyMsgId
		kv.config = config
		kv.receiveConfigNum = receiveConfigNum
		kv.migratingShards = migratingShards
		kv.nextMsgId = nextMsgId
		// labutil.PrintMessage(kv.selfString() + "readSnapshot, config = " + fmt.Sprint(kv.config.Num))
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

func (kv *ShardKV) getNextMsgId() ClerkMsgId {
	return kv.nextMsgId + 1
}

// must have outer lock!
func (kv *ShardKV) queryConfig(num int) (shardmaster.Config, bool) {
	var config shardmaster.Config
	ctx, cancel := context.WithTimeout(context.Background(), QueryConfigTimeout)
	defer cancel()

	done := make(chan bool, 1)

	go func() {
		config = kv.mck.Query(num)
		done <- true
	}()

	select {
	case <-ctx.Done():
		return shardmaster.Config{}, true
	case <-done:
		return config, false
	}
}

func (kv *ShardKV) checkReconfig() {
	killed := kv.killed()
	kv.lock()

	// only leader check reconfig
	_, isLeader := kv.rf.GetState()
	if !isLeader || killed {
		kv.unlock()
		return
	}

	latestConfig, timeout := kv.queryConfig(-1)
	if timeout {
		kv.unlock()
		return
	}

	// get config first (must use internal request) (TODO: timeout retry)
	kv.unlock()
	curConfig := kv.CallGetConfig()
	kv.lock()

	if curConfig.Num < latestConfig.Num {
		// process re-configurations one at a time, in order
		nextConfig, timeout := kv.queryConfig(curConfig.Num + 1)
		if timeout {
			kv.unlock()
			return
		}

		// new group list
		newGroupList := make([]int, 0)
		for gid := range nextConfig.Groups {
			found := false
			for oldGid := range curConfig.Groups {
				if gid == oldGid {
					found = true
					break
				}
			}
			if !found {
				newGroupList = append(newGroupList, gid)
			}
		}

		// all group list
		allGroupList := make([]int, 0)
		for gid := range curConfig.Groups {
			allGroupList = append(allGroupList, gid)
		}
		allGroupList = append(allGroupList, newGroupList...)
		// fmt.Println(kv.selfString() + "CurconfigNum = " + fmt.Sprint(curConfig.Num) + ", NextconfigNum = " + fmt.Sprint(nextConfig.Num) + ", LatestconfigNum = " + fmt.Sprint(latestConfig.Num) + ", NewGroupList = " + fmt.Sprint(newGroupList) + ", AllGroupList = " + fmt.Sprint(allGroupList))

		// isOutSideGroup: if allGroupList does not contain kv.gid (an "outside" but live server, no need to migrate shards or wait for receive, only need to update config)
		isOutSideGroup := true
		for _, gid := range allGroupList {
			if gid == kv.gid {
				isOutSideGroup = false
				break
			}
		}

		deleteShards := make([]int, 0)
		if !isOutSideGroup {
			oldShardList := getShardListOf(curConfig, kv.gid)
			nextShardList := getShardListOf(nextConfig, kv.gid)
			// get deletedShards and addedShards
			migratedShards := make([]int, 0)
			for _, shard := range oldShardList {
				found := false
				for _, newShard := range nextShardList {
					if shard == newShard {
						found = true
						break
					}
				}
				if !found {
					migratedShards = append(migratedShards, shard)
				}
			}

			// print oldShardList, nextShardList, migratedShards
			// labutil.PrintMessage(kv.selfString() + "CurconfigNum = " + fmt.Sprint(curConfig.Num) + ", NextconfigNum = " + fmt.Sprint(nextConfig.Num) + ", OldShardList = " + fmt.Sprint(oldShardList) + ", nextShardList = " + fmt.Sprint(nextShardList) + ", MigratedShards = " + fmt.Sprint(migratedShards))

			// save variables before unlock
			// oldConfig := curConfig
			kvGid := kv.gid
			oldConfig := curConfig
			kv.unlock()

			// group shard in migratedShards according tonextConfig.Shards[shard]
			migratedShardsMap := make(map[int][]int)
			for _, shard := range migratedShards {
				nextGid := nextConfig.Shards[shard]
				if _, ok := migratedShardsMap[nextGid]; !ok {
					migratedShardsMap[nextGid] = make([]int, 0)
				}
				migratedShardsMap[nextGid] = append(migratedShardsMap[nextGid], shard)
			}

			// migrate migratedShards to other groups
			for _, toGid := range allGroupList {
				if kv.killed() {
					return
				}
				if toGid == kvGid {
					continue
				}
				shards := migratedShardsMap[toGid]
				isNewGroup := false
				for _, newGid := range newGroupList {
					if toGid == newGid {
						isNewGroup = true
						break
					}
				}

				// shards might be empty, just to inform other group in that case
				// labutil.PrintMessage(kv.selfString() + "Migrate shard = " + fmt.Sprint(shards) + " from gid = " + fmt.Sprint(kvGid) + " to gid = " + fmt.Sprint(toGid) + ", nextConfig.Num = " + fmt.Sprint(nextConfig.Num))
				if len(shards) > 0 {
					migratingShards := kv.CallGetMigratingShards()
					// join shards, avoid duplicate
					for _, shard := range shards {
						found := false
						for _, s := range migratingShards {
							if s == shard {
								found = true
								break
							}
						}
						if !found {
							migratingShards = append(migratingShards, shard)
						}
					}
					kv.CallUpdateMigratingShards(migratingShards)
				}
				// TODO: callMigrateShards in using go routines
				kv.callMigrateShards(shards, kvGid, toGid, oldConfig, nextConfig, isNewGroup)
				deleteShards = append(deleteShards, shards...)
			}

			for !kv.killed() {
				if !kv.receiveFromAllGroups(curConfig, allGroupList) {
					time.Sleep(WaitMigrateTimeOut)
				} else {
					break
				}
			}
		} else {
			kv.unlock()
		}

		if kv.killed() {
			return
		}
		// update config at last (delete unused shards and reset migratingShards empty will be done here atomically!)
		kv.CallUpdateConfig(nextConfig, deleteShards)
		// fmt.Println(kv.selfString() + "UpdateConfig success, curConfig.Num = " + fmt.Sprint(curConfig.Num) + ", nextConfig.Num = " + fmt.Sprint(nextConfig.Num))
	} else {
		kv.unlock()
	}
}

func (kv *ShardKV) receiveFromAllGroups(curConfig shardmaster.Config, groupList []int) bool {
	receiveConfigNum := kv.callGetReceiveConfigNum()
	for _, gid := range groupList {
		if gid == kv.gid {
			continue
		}
		if configNum, ok := receiveConfigNum[gid]; !ok || configNum < curConfig.Num+1 { // curConfig.Num+1 == nextConfig.Num
			// fmt.Println(kv.selfString() + "Not receive from gid = " + fmt.Sprint(gid) + ", configNum = " + fmt.Sprint(configNum) + ", kv.nextconfig.Num = " + fmt.Sprint(curConfig.Num+1))
			return false
		}
	}
	return true
}

func (kv *ShardKV) callMigrateShards(shards []int, fromGid int, toGid int, oldConfig shardmaster.Config, nextConfig shardmaster.Config, isNewGroup bool) {
	// get shard data (must use internal request) (TODO: timeout retry)
	kv.lock()

	shardsData := make(map[string]string)
	if len(shards) > 0 {
		kv.unlock()
		shardsData = kv.CallGetShardsData(shards)
		kv.lock()
		for key, value := range shardsData {
			shardsData[key] = value
		}
	}

	// send data to new group
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()

	if kv.killed() {
		return // neccessary! avoid "ghost" migration (killed during get shards data, but still migrate empty data)
	}

	// fmt.Println(kv.selfString()+"Call Migrate shard = "+fmt.Sprint(shards)+" from gid = "+fmt.Sprint(fromGid)+" to gid = "+fmt.Sprint(toGid)+", msgId = "+fmt.Sprint(kv.nextMsgId)+", nextConfig.Num = "+fmt.Sprint(nextConfig.Num), ", shardsData = "+fmt.Sprint(shardsData))

	migrateShardsArgs := MigrateShardsArgs{ShardsKvData: shardsData, ConfigNum: nextConfig.Num, FromGid: fromGid, IsNewGroup: isNewGroup, ClientId: kv.clientId, MsgId: kv.nextMsgId} // curConfig.NUm = nextConfig.Num
	allToServers := oldConfig.Groups[toGid]
	if len(allToServers) == 0 {
		allToServers = nextConfig.Groups[toGid]
	}

	// note: len(allToServers) must > 0
	leaderId := 0
	for {
		srv := kv.make_end(allToServers[leaderId])
		var migrateShardsReply MigrateShardsReply
		ok := srv.Call("ShardKV.MigrateShards", &migrateShardsArgs, &migrateShardsReply)
		if !ok {
			leaderId = (leaderId + 1) % len(allToServers)
			time.Sleep(TryNextGroupServerInterval)
			// fmt.Println("net error not ok")
			continue
		}

		switch migrateShardsReply.Err {
		case OK:
			return
		case ErrConfigNotMatch:
			time.Sleep(WaitForConfigConsistentTimeOut)
			continue
		case ErrAlreadyMigrated:
			return // give up if already migrated
		case ErrWrongLeader:
			leaderId = (leaderId + 1) % len(allToServers)
			time.Sleep(TryNextGroupServerInterval)
			continue
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval)
			continue
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.outPutCh = make(map[TypeOpId]chan ExeResult)

	// persisted variables, will be reconvered if there is a snapshot
	kv.kvData = make(map[string]string)
	kv.lastApplyMsgId = make(map[TypeClientId]ClerkMsgId)
	kv.config = shardmaster.Config{}
	kv.receiveConfigNum = make(map[int]int)
	kv.migratingShards = make([]int, 0)
	kv.clientId = TypeClientId(gid) // use gid as clientId(servers of the same group must have the same clientId)
	kv.nextMsgId = 0

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.checkReconfigTimer = time.NewTimer(0)

	// randID is a randon number between 100 to 999
	kv.randID = 100 + nrand()%900

	kv.readSnapshot(persister.ReadSnapshot())

	// fmt.Println(kv.selfString()+"StartServer", "migratingShards = ", kv.migratingShards, ", lastApplyMsgId = ", kv.lastApplyMsgId, ", kvDataSize = "+fmt.Sprint(len(kv.kvData))+", configNum = "+fmt.Sprint(kv.config.Num))

	go kv.waitApply()

	go kv.periodicCheckReconfig()

	return kv
}
