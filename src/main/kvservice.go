package main

import (
	"../kvraft"
	"../labrpc"
	"../raft"
)

type KVServive struct {
	serverNum    int
	maxraftstate int
	ends         []*labrpc.ClientEnd

	kvservers  []*kvraft.KVServer
	persisters []*raft.Persister
}

func StartKVService(serverNum int, maxraftstate int, ends []*labrpc.ClientEnd) *KVServive {
	kvs := new(KVServive)
	kvs.serverNum = serverNum
	kvs.maxraftstate = maxraftstate
	kvs.ends = ends

	kvs.kvservers = make([]*kvraft.KVServer, serverNum)
	kvs.persisters = make([]*raft.Persister, serverNum)

	for i := 0; i < serverNum; i++ {
		kvs.persisters[i] = raft.MakePersister()
		kvs.kvservers[i] = kvraft.StartKVServer(kvs.ends, i, kvs.persisters[i], maxraftstate)
	}

	return kvs
}

func main() {
	serverNum := 5
	maxraftstate := 1000

	net := labrpc.MakeNetwork()
	endnames := make([]string, serverNum)
	ends := make([]*labrpc.ClientEnd, serverNum)

	// for j := 0; j < serverNum; j++ {
	// 	ends[j] = net.MakeEnd(cfg.endnames[i][j])
	// 	cfg.net.Connect(endnames[i][j], j)
	// }

	KVServive := StartKVService(serverNum, maxraftstate, ends)

	endsCh := make(chan []*labrpc.ClientEnd, 1)
	endsCh <- KVServive.ends
}
