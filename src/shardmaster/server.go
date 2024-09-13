package shardmaster

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../labutil"
	"../raft"
)

const WaitOpTimeOut = time.Millisecond * 500

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	stopCh   chan struct{}
	outPutCh map[TypeOpId]chan ExeResult
	dead     int32 // set by Kill()
	nextOpId TypeOpId

	configs        []Config                    // indexed by config num
	lastApplyMsgId map[TypeClientId]ClerkMsgId //avoid duplicate apply
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ExeResult struct {
	Config Config
	Err    Err
}

type Op struct {
	// Your data here.
	Method    Method
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs

	ClientId TypeClientId
	MsgId    ClerkMsgId

	// ServerId + OpId is unique for each op
	ServerId int
	OpId     TypeOpId
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.lock()

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.unlock()
		return
	}
	labutil.PrintMessage(fmt.Sprintf("Join: %v", args.Servers))

	sm.nextOpId = sm.getNextOpId()
	// copy join args
	joinArgs := JoinArgs{}
	joinArgs.Servers = make(map[int][]string)
	for gid, servers := range args.Servers {
		joinArgs.Servers[gid] = servers
	}
	joinArgs.ClientId = args.ClientId
	joinArgs.MsgId = args.MsgId
	op := Op{
		Method:    JOIN,
		JoinArgs:  joinArgs,
		LeaveArgs: LeaveArgs{},
		MoveArgs:  MoveArgs{},
		QueryArgs: QueryArgs{},
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  sm.me,
		OpId:      sm.nextOpId,
	}
	sm.unlock()

	res := sm.waitOp(op)
	reply.WrongLeader = false
	reply.Err = res.Err
	labutil.PrintMessage(fmt.Sprintf("Join Success: %v", args.Servers))
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.lock()

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.unlock()
		return
	}
	labutil.PrintMessage(fmt.Sprintf("Leave: %v", args.GIDs))

	sm.nextOpId = sm.getNextOpId()
	op := Op{
		Method:    LEAVE,
		JoinArgs:  JoinArgs{},
		LeaveArgs: LeaveArgs{args.GIDs, args.ClientId, args.MsgId},
		MoveArgs:  MoveArgs{},
		QueryArgs: QueryArgs{},
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  sm.me,
		OpId:      sm.nextOpId,
	}
	sm.unlock()

	res := sm.waitOp(op)
	reply.WrongLeader = false
	reply.Err = res.Err
	labutil.PrintMessage(fmt.Sprintf("Leave Success: %v", args.GIDs))
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.lock()

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.unlock()
		return
	}

	sm.nextOpId = sm.getNextOpId()
	op := Op{
		Method:    MOVE,
		JoinArgs:  JoinArgs{},
		LeaveArgs: LeaveArgs{},
		MoveArgs:  MoveArgs{args.Shard, args.GID, args.ClientId, args.MsgId},
		QueryArgs: QueryArgs{},
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  sm.me,
		OpId:      sm.nextOpId,
	}
	sm.unlock()

	res := sm.waitOp(op)
	reply.WrongLeader = false
	reply.Err = res.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.lock()

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.unlock()
		return
	}

	sm.nextOpId = sm.getNextOpId()
	op := Op{
		Method:    QUERY,
		JoinArgs:  JoinArgs{},
		LeaveArgs: LeaveArgs{},
		MoveArgs:  MoveArgs{},
		QueryArgs: QueryArgs{args.Num, args.ClientId, args.MsgId},
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ServerId:  sm.me,
		OpId:      sm.nextOpId,
	}
	sm.unlock()

	res := sm.waitOp(op)
	reply.Err = res.Err
	reply.WrongLeader = false
	reply.Config = res.Config
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.lock()
	defer sm.unlock()
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	sm.lock()
	defer sm.unlock()
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.nextOpId = 0

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.lastApplyMsgId = make(map[TypeClientId]ClerkMsgId)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.stopCh = make(chan struct{})
	sm.outPutCh = make(map[TypeOpId]chan ExeResult)

	sm.configs[0].Num = InvalidConfigNum
	sm.configs[0].Shards = [NShards]int{InvalidGID}

	go sm.waitApply()

	return sm
}

func (sm *ShardMaster) lock() {
	sm.mu.Lock()
}

func (sm *ShardMaster) unlock() {
	sm.mu.Unlock()
}

// example: distributeEvenly(10, 4) -> [3, 3, 2, 2]
func (sm *ShardMaster) distributeEvenly(N int, m int) []int {
	base := N / m
	remainder := N % m

	result := make([]int, m)

	for i := 0; i < m; i++ {
		if i < remainder {
			result[i] = base + 1
		} else {
			result[i] = base
		}
	}

	return result
}

func (sm *ShardMaster) moveShards(shards []int, groupList []int, targetGroupSizes []int) []int {
	if len(groupList) != len(targetGroupSizes) {
		panic("moveShards: groupList and targetGroupSizes have different lengths")
	}

	shardsNum := len(shards)
	// groupNum := len(targetGroupSizes)

	newShards := make([]int, shardsNum)
	groupToShardCount := make(map[int]int)

	// Count the number of shards in each group
	for _, shard := range shards {
		if shard != 0 {
			groupToShardCount[shard]++
		}
	}

	// Sort the groupList and targetGroupSizes based on old group sizes in descending order
	sort.SliceStable(groupList, func(i, j int) bool {
		return groupToShardCount[groupList[i]] > groupToShardCount[groupList[j]]
	})

	// Assign shards to groups in order to minimize movements
	// shardIndex := 0
	for i, group := range groupList {
		targetSize := targetGroupSizes[i]
		currentCount := groupToShardCount[group]

		if currentCount >= targetSize {
			// Assign the minimum required shards to this group
			for j := 0; j < shardsNum && targetSize > 0; j++ {
				if shards[j] == group {
					newShards[j] = group
					targetSize--
				}
			}
		} else {
			// Assign all existing shards of this group
			for j := 0; j < shardsNum && currentCount > 0; j++ {
				if shards[j] == group {
					newShards[j] = group
					currentCount--
				}
			}

			// Assign additional shards from unassigned or other groups
			for j := 0; j < shardsNum && targetSize > 0; j++ {
				if newShards[j] == 0 && shards[j] != group {
					newShards[j] = group
					targetSize--
				}
			}
		}
	}

	// Assign any remaining unassigned shards to any group that still has space
	for i := 0; i < shardsNum; i++ {
		if newShards[i] == 0 {
			for j, group := range groupList {
				if targetGroupSizes[j] > 0 {
					newShards[i] = group
					targetGroupSizes[j]--
					break
				}
			}
		}
	}

	return newShards
}

func (sm *ShardMaster) getNextOpId() TypeOpId {
	//return sm.nextOpId + 1
	return TypeOpId(nrand()) //assume to be unique
}

func (sm *ShardMaster) deleteOutputCh(opId TypeOpId) {
	delete(sm.outPutCh, opId)
}

func (sm *ShardMaster) waitOp(op Op) (res ExeResult) {
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	waitOpTimer := time.NewTimer(WaitOpTimeOut)

	ch := make(chan ExeResult, 1) //ch will be deleted if it receives the result from Raft module as a leader, or timeout

	sm.lock()
	sm.outPutCh[op.OpId] = ch
	sm.unlock()

	for {
		select {
		case res_ := <-ch:
			res.Err = res_.Err
			res.Config = res_.Config
			sm.lock()
			sm.deleteOutputCh(op.OpId)
			sm.unlock()
			return
		case <-waitOpTimer.C:
			res.Err = ErrTimeout
			sm.lock()
			sm.deleteOutputCh(op.OpId)
			sm.unlock()
			return
		}
	}
}

func (sm *ShardMaster) waitApply() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}

			op := msg.Command.(Op)
			ExeResult := ExeResult{Err: OK}

			sm.lock()

			lastMsgId, ok := sm.lastApplyMsgId[op.ClientId]
			isApplied := ok && lastMsgId >= op.MsgId

			switch op.Method {
			case JOIN:
				if !isApplied {
					oldConfig := sm.configs[len(sm.configs)-1]
					// copy old config
					newConfig := Config{}
					newConfig.Num = len(sm.configs)
					newConfig.Groups = map[int][]string{}
					for gid, servers := range oldConfig.Groups {
						newConfig.Groups[gid] = servers
					}
					for gid, servers := range op.JoinArgs.Servers {
						newConfig.Groups[gid] = servers
					}

					if len(newConfig.Groups) == 0 {
						labutil.PrintWarning("No group in new config after join")
						newConfig.Shards = [NShards]int{InvalidGID}
					} else {
						targetGroupSizes := sm.distributeEvenly(NShards, len(newConfig.Groups))
						groupList := make([]int, 0, len(newConfig.Groups))
						for gid := range newConfig.Groups {
							groupList = append(groupList, gid)
						}

						newShardsArray := sm.moveShards(oldConfig.Shards[:], groupList, targetGroupSizes)
						copy(newConfig.Shards[:], newShardsArray[:])
					}
					sm.configs = append(sm.configs, newConfig)

					sm.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case LEAVE:
				if !isApplied {
					oldConfig := sm.configs[len(sm.configs)-1]
					// copy old config
					newConfig := Config{}
					newConfig.Num = len(sm.configs)
					newConfig.Groups = map[int][]string{}
					for gid, servers := range oldConfig.Groups {
						newConfig.Groups[gid] = servers
					}
					for _, gid := range op.LeaveArgs.GIDs {
						delete(newConfig.Groups, gid)
					}

					if len(newConfig.Groups) == 0 {
						newConfig.Shards = [NShards]int{InvalidGID}
					} else {
						targetGroupSizes := sm.distributeEvenly(NShards, len(newConfig.Groups))
						groupList := make([]int, 0, len(newConfig.Groups))
						for gid := range newConfig.Groups {
							groupList = append(groupList, gid)
						}

						newShardsArray := sm.moveShards(oldConfig.Shards[:], groupList, targetGroupSizes)
						copy(newConfig.Shards[:], newShardsArray[:])
					}
					sm.configs = append(sm.configs, newConfig)

					sm.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case MOVE:
				if !isApplied {
					oldConfig := sm.configs[len(sm.configs)-1]
					// copy old config
					newConfig := Config{}
					newConfig.Num = len(sm.configs)
					newConfig.Groups = map[int][]string{}
					for gid, servers := range oldConfig.Groups {
						newConfig.Groups[gid] = servers
					}

					for i := 0; i < NShards; i++ {
						if oldConfig.Shards[i] == op.MoveArgs.Shard {
							newConfig.Shards[i] = op.MoveArgs.GID
						} else {
							newConfig.Shards[i] = oldConfig.Shards[i]
						}
					}
					sm.configs = append(sm.configs, newConfig)

					sm.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			case QUERY:
				if op.QueryArgs.Num == -1 || op.QueryArgs.Num >= len(sm.configs) {
					ExeResult.Config = sm.configs[len(sm.configs)-1]
				} else {
					ExeResult.Config = sm.configs[op.QueryArgs.Num]
				}
				if !isApplied {
					sm.lastApplyMsgId[op.ClientId] = op.MsgId
				}
			}

			ch, ok := sm.outPutCh[op.OpId]
			if ok && op.ServerId == sm.me {
				ch <- ExeResult
			}
			sm.unlock()
		}
	}
}
