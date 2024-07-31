package shardmaster

import (
	"sort"
	"sync"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.lock()
	defer sm.unlock()

	oldConfig := sm.configs[len(sm.configs)-1]
	// copy old config
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = map[int][]string{}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	targetGroupSizes := distributeEvenly(NShards, len(newConfig.Groups))
	groupList := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		groupList = append(groupList, gid)
	}

	newShardsArray := moveShards(oldConfig.Shards[:], groupList, targetGroupSizes)
	copy(newConfig.Shards[:], newShardsArray[:])

	sm.configs = append(sm.configs, newConfig)

	reply.WrongLeader = false
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.lock()
	defer sm.unlock()

	oldConfig := sm.configs[len(sm.configs)-1]
	// copy old config
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = map[int][]string{}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}

	if len(newConfig.Groups) == 0 {
		newConfig.Shards = [NShards]int{InvalidGID}
	} else {
		targetGroupSizes := distributeEvenly(NShards, len(newConfig.Groups))
		groupList := make([]int, 0, len(newConfig.Groups))
		for gid := range newConfig.Groups {
			groupList = append(groupList, gid)
		}

		newShardsArray := moveShards(oldConfig.Shards[:], groupList, targetGroupSizes)
		copy(newConfig.Shards[:], newShardsArray[:])
	}

	sm.configs = append(sm.configs, newConfig)

	reply.WrongLeader = false
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.lock()
	defer sm.unlock()

	oldConfig := sm.configs[len(sm.configs)-1]
	// copy old config
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = map[int][]string{}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for i := 0; i < NShards; i++ {
		if oldConfig.Shards[i] == args.Shard {
			newConfig.Shards[i] = args.GID
		} else {
			newConfig.Shards[i] = oldConfig.Shards[i]
		}
	}

	sm.configs = append(sm.configs, newConfig)

	reply.WrongLeader = false
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.lock()
	defer sm.unlock()

	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	reply.WrongLeader = false
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.lock()
	defer sm.unlock()
	sm.rf.Kill()
	// Your code here, if desired.
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

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.configs[0].Num = InvalidConfigNum
	sm.configs[0].Shards = [NShards]int{InvalidGID}

	return sm
}

func (sm *ShardMaster) lock() {
	sm.mu.Lock()
}

func (sm *ShardMaster) unlock() {
	sm.mu.Unlock()
}

// example: distributeEvenly(10, 4) -> [3, 3, 2, 2]
func distributeEvenly(N int, m int) []int {
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

func moveShards(shards []int, groupList []int, targetGroupSizes []int) []int {
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

	// Sort the groupList and targetGroupSizes based on group sizes in descending order
	sort.SliceStable(groupList, func(i, j int) bool {
		return targetGroupSizes[i] > targetGroupSizes[j]
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
