package shardkv

import (
	"time"

	"../shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrConfigNotMatch = "ErrConfigNotMatch"
)

const (
	WaitForConfigConsistentTimeOut = time.Millisecond * 1000
	TryNextGroupServerInterval     = time.Millisecond * 50
)

type Err string

type ClerkMsgId int64

type TypeClientId int64

type TypeOpId int64

type Method string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId TypeClientId
	MsgId    ClerkMsgId

	Shard int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId TypeClientId
	MsgId    ClerkMsgId

	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardsArgs struct {
	ShardsKvData map[string]string
	ConfigNum    int
	FromGid      int
	IsNewGroup   bool
	ClientId     TypeClientId
	MsgId        ClerkMsgId
}

type MigrateShardsReply struct {
	Err Err
}

type UpdateConfigArgs struct {
	Config   shardmaster.Config
	ClientId TypeClientId
	MsgId    ClerkMsgId
}

type UpdateConfigReply struct {
	Err Err
}

type GetConfigArgs struct {
	ClientId TypeClientId
	MsgId    ClerkMsgId
}

type GetConfigReply struct {
	Err    Err
	Config shardmaster.Config
}

type GetShardsDataArgs struct {
	Shards   []int
	ClientId TypeClientId
	MsgId    ClerkMsgId
}

type GetShardsDataReply struct {
	Err        Err
	ShardsData map[string]string
}
