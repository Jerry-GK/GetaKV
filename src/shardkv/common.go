package shardkv

import "../shardmaster"

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
	ErrOutdatedConfig = "ErrOutdatedConfig"
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
	Gid   int
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
	Gid   int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	ShardKvData map[string]string
	ConfigNum   int
	OldGid      int
	ClientId    TypeClientId
	MsgId       ClerkMsgId
}

type MigrateReply struct {
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
