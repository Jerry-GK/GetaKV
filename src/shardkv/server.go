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
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	MIGRATESHARDS = "MigrateShards"
	UPDATECONFIG  = "UpdateConfig"
	GETCONFIG     = "GetConfig"
	GETSHARDSDATA = "GetShardsData"
)

type ExeResult struct {
	Err          Err
	Value        string
	Config       shardmaster.Config
	ShardsKvData map[string]string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Method Method

	Shard int

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
	Config shardmaster.Config

	// for GetShardsData
	GetShards []int
}

type ShardKV struct {
	mu           sync.Mutex
	mu_reconfig  sync.Mutex
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

	clientId  TypeClientId
	nextMsgId ClerkMsgId

	checkReconfigTimer *time.Timer
}

func (kv *ShardKV) selfString() string {
	// Group[gid] - Server[me]
	s := "Group[" + fmt.Sprint(kv.gid) + "] - Server[" + fmt.Sprint(kv.me) + "]: "
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

func (kv *ShardKV) lockReconfig() {
	// labutil.PrintMessage(kv.selfString() + "lock muReconfig")
	kv.mu_reconfig.Lock()
}

func (kv *ShardKV) unlockReconfig() {
	// labutil.PrintMessage(kv.selfString() + "unlock muReconfig")
	kv.mu_reconfig.Unlock()
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

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   GET,
		Shard:    args.Shard,
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
	kv.lockReconfig()
	defer kv.unlockReconfig() // must unlock at the end, unlock earlier might make migration lose data

	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.unlock()
		return
	}

	fmt.Println(kv.selfString() + " try to " + args.Op + " " + args.Key + " " + args.Value)

	kv.nextOpId = kv.getNextOpId()
	op := Op{
		Method:   Method(args.Op),
		Shard:    args.Shard,
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
			res.Config = res_.Config
			res.ShardsKvData = res_.ShardsKvData
			kv.lock()
			kv.deleteOutputCh(op.OpId)
			kv.unlock()
			return
		case <-waitOpTimer.C:
			res.Err = ErrTimeout
			if op.Method == GETSHARDSDATA {
				fmt.Println("timeout!")
			}
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
				if kv.config.Shards[op.Shard] != kv.gid {
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
					if kv.config.Shards[op.Shard] != kv.gid {
						ExeResult.Err = ErrWrongGroup
						break
					}
					kv.kvData[op.Key] = op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case APPEND:
				if !isApplied {
					if kv.config.Shards[op.Shard] != kv.gid {
						ExeResult.Err = ErrWrongGroup
						break
					}
					// get the last 6 chars of kvData[op.Key]
					lastValue := kv.kvData[op.Key]
					if len(lastValue) >= 6 {
						lastValue = lastValue[len(lastValue)-6:]
						if lastValue == op.Value {
							fmt.Println(kv.selfString() + "!!!!!! key = " + op.Key + ", value = " + op.Value + ", Config = " + fmt.Sprint(kv.config))
						}
					}
					kv.kvData[op.Key] += op.Value
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case MIGRATESHARDS:
				if !isApplied {
					// check kv.config.Num == op.ConfigNum-1 to process reconfig one at a time
					// new group does not need to check configNum
					if kv.config.Num == op.ConfigNum-1 || op.IsNewGroup {
						for key, value := range op.ShardsKvData {
							kv.kvData[key] = value
							fmt.Println(kv.selfString() + "Migrate key = " + key + ", value = " + value + ", shard = " + fmt.Sprint(op.Shard) + ", from gid = " + fmt.Sprint(op.OldGid) + " , nextCondifNum = " + fmt.Sprint(op.ConfigNum))
						}
						kv.receiveConfigNum[op.OldGid] = op.ConfigNum
						// fmt.Println(kv.selfString() + "Migrate success, kv.config.Num = " + fmt.Sprint(kv.config.Num) + ", op.ConfigNum = " + fmt.Sprint(op.ConfigNum) + ", oldGid = " + fmt.Sprint(op.OldGid) + ", msgId = " + fmt.Sprint(op.MsgId))
					} else {
						ExeResult.Err = ErrConfigNotMatch
						fmt.Println(kv.selfString() + "Migrate failed, kv.config.Num = " + fmt.Sprint(kv.config.Num) + ", op.ConfigNum = " + fmt.Sprint(op.ConfigNum) + ", oldGid = " + fmt.Sprint(op.OldGid) + ", msgId = " + fmt.Sprint(op.MsgId))
						break // do not update lastApplyMsgId
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
						for _, gid := range leftGroupList {
							delete(kv.receiveConfigNum, gid)
						}

						kv.config = op.Config
					}
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			// two internal request
			case GETCONFIG:
				if !isApplied {
					ExeResult.Config = kv.config
					kv.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case GETSHARDSDATA:
				if !isApplied {
					shardsKvData := make(map[string]string)
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
				} else {
					fmt.Println("already applied!")
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
	kv.lockReconfig()
	defer kv.unlockReconfig()
	kv.lock()

	// only leader check reconfig
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.unlock()
		return
	}

	fmt.Println(kv.selfString() + "leader checkReconfig, curConfig.Num = " + fmt.Sprint(kv.config.Num))
	defer fmt.Println(kv.selfString() + "leader checkReconfig done")

	latestConfig, timeout := kv.queryConfig(-1)
	if timeout {
		fmt.Println(kv.selfString() + "leader query timeout")
		kv.unlock()
		return
	}

	// get config first (must use internal request) (TODO: timeout retry)
	kv.nextMsgId = kv.getNextMsgId()
	getConfigArgs := GetConfigArgs{ClientId: kv.clientId, MsgId: kv.nextMsgId}
	var getConfigReply GetConfigReply
	kv.unlock()
	fmt.Println(kv.selfString() + "leader try to get config ")
	kv.GetConfig(&getConfigArgs, &getConfigReply)
	fmt.Println(kv.selfString() + "leader got config ")
	kv.lock()
	curConfig := getConfigReply.Config

	if curConfig.Num < latestConfig.Num {
		fmt.Println(kv.selfString() + "try to update config, curConfig.Num = " + fmt.Sprint(curConfig.Num))
		// process re-configurations one at a time, in order
		nextConfig, timeout := kv.queryConfig(curConfig.Num + 1)
		if timeout {
			kv.unlock()
			fmt.Println(kv.selfString() + "check reconfig timeout")
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
				kv.callMigrateShards(shards, kvGid, toGid, oldConfig, nextConfig, isNewGroup)
			}

			for !kv.killed() {
				if !kv.receiveFromAllGroups(curConfig, allGroupList) {
					fmt.Println("wait")
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
		// update config at last
		kv.lock()
		kv.nextMsgId = kv.getNextMsgId()
		args := UpdateConfigArgs{Config: nextConfig, ClientId: kv.clientId, MsgId: kv.nextMsgId}
		var reply UpdateConfigReply
		kv.unlock()
		kv.UpdateConfig(&args, &reply)
		fmt.Println(kv.selfString() + "UpdateConfig success, curConfig.Num = " + fmt.Sprint(curConfig.Num) + ", nextConfig.Num = " + fmt.Sprint(nextConfig.Num))
	} else {
		kv.unlock()
	}
}

func (kv *ShardKV) receiveFromAllGroups(curConfig shardmaster.Config, groupList []int) bool {
	kv.lock()
	defer kv.unlock()
	for _, gid := range groupList {
		if gid == kv.gid {
			continue
		}
		if configNum, ok := kv.receiveConfigNum[gid]; !ok || configNum < curConfig.Num+1 { // curConfig.Num+1 == nextConfig.Num
			fmt.Println(kv.selfString() + "Not receive from gid = " + fmt.Sprint(gid) + ", configNum = " + fmt.Sprint(configNum) + ", kv.nextconfig.Num = " + fmt.Sprint(curConfig.Num+1))
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
		kv.nextMsgId = kv.getNextMsgId()
		getShardsDataArgs := GetShardsDataArgs{Shards: shards, ClientId: kv.clientId, MsgId: kv.nextMsgId}
		var getShardsDataReply GetShardsDataReply
		kv.unlock()
		kv.GetShardsData(&getShardsDataArgs, &getShardsDataReply)
		kv.lock()
		for key, value := range getShardsDataReply.ShardsData {
			shardsData[key] = value
		}
	}

	fmt.Println(kv.selfString() + "Call Migrate shard = " + fmt.Sprint(shards) + " from gid = " + fmt.Sprint(fromGid) + " to gid = " + fmt.Sprint(toGid) + ", nextConfig.Num = " + fmt.Sprint(nextConfig.Num))
	fmt.Println("ShardsKvData = ", shardsData)

	// send data to new group
	kv.nextMsgId = kv.getNextMsgId()
	kv.unlock()

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
			fmt.Println("net error not ok")
			continue
		}

		switch migrateShardsReply.Err {
		case OK:
			return
		case ErrConfigNotMatch:
			time.Sleep(WaitForConfigConsistentTimeOut)
			continue
		case ErrWrongLeader:
			leaderId = (leaderId + 1) % len(allToServers)
			time.Sleep(TryNextGroupServerInterval)
			fmt.Println("wrong leader")
			continue
		case ErrTimeout:
			time.Sleep(TryNextGroupServerInterval)
			fmt.Println("net error timeout")
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

	fmt.Println(kv.selfString() + "StartServer")
	defer fmt.Println(kv.selfString() + "StartServer end")

	// Your initialization code here.
	kv.nextOpId = 0
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)
	kv.receiveConfigNum = make(map[int]int)

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

// Shard KV RPCs
func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
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

// only for internal
func (kv *ShardKV) GetConfig(args *GetConfigArgs, reply *GetConfigReply) {
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
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

// only for internal
func (kv *ShardKV) GetShardsData(args *GetShardsDataArgs, reply *GetShardsDataReply) {
	kv.lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
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
