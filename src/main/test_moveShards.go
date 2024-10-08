package main

import (
	"fmt"
	"sort"
)

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

func main() {
	fmt.Println(moveShards(
		[]int{1000, 1001, 1003, 1004, 1005, 1006, 1008, 1009, 1009, 2008},
		[]int{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})) // Example 1
	// sfmt.Println(moveShards([]int{9, 9, 17}, []int{10}, []int{3}))      // Example 2
}
