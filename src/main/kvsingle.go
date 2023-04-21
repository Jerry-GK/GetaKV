package main

import (
	"bufio"
	"fmt"
	"os"

	"../kvraft"
	"../labrpc"
	"../labutil"
	"../parser"
	"../raft"
)

type KVServive struct {
	serverNum    int
	maxraftstate int
	ends         []*labrpc.ClientEnd

	kvservers  []*kvraft.KVServer
	persisters []*raft.Persister
}

func (kvs *KVServive) Kill() {
	for i := 0; i < kvs.serverNum; i++ {
		kvs.kvservers[i].Kill()
	}
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
	//make service
	serverNum := 5
	maxraftstate := 1000

	// net := labrpc.MakeNetwork()
	endnames := make([]string, serverNum)
	for j := 0; j < serverNum; j++ {
		endnames[j] = fmt.Sprint("Server-", j)
	}

	net := labrpc.MakeNetwork()

	ends := make([]*labrpc.ClientEnd, serverNum)

	for j := 0; j < serverNum; j++ {
		ends[j] = net.MakeEnd(endnames[j])
		net.Connect(endnames[j], j)
		net.Enable(endnames[j], true)
	}

	kvs := StartKVService(serverNum, maxraftstate, ends)

	for i := 0; i < serverNum; i++ {
		kvsvc := labrpc.MakeService(kvs.kvservers[i])
		rfsvc := labrpc.MakeService(kvs.kvservers[i].GetRf())
		srv := labrpc.MakeServer()
		srv.AddService(kvsvc)
		srv.AddService(rfsvc)
		net.AddServer(i, srv)
	}

	labutil.PrintDirect("\n======GetaKV Start======")

	//make single client
	ck := kvraft.MakeClerk(ends)
	reader := bufio.NewReader(os.Stdin)
	quit := false

	//make parser
	ps := parser.MakeParser()

	for !quit {
		//input command
		labutil.PrintDirect("\n\nGetaKV > ")
		input := ""

		line, _, _ := reader.ReadLine()
		input = string(line)

		cmd := ps.Parse(input)

		switch cmd.Op {
		case "PUT":
			ck.Put(cmd.Key, cmd.Value)
			labutil.PrintDirect("Put Success")
		case "APPEND":
			ck.Append(cmd.Key, cmd.Value)
			labutil.PrintDirect("Append Success")
		case "GET":
			value := ck.Get(cmd.Key)
			if value == "" {
				labutil.PrintDirect("Get Failed: Key Not Found")
			} else {
				labutil.PrintDirect("Get Success: Value = " + value)
			}
		case "QUIT":
			quit = true
		case "INVALID":
			labutil.PrintDirect("Invalid Command! Error: " + cmd.Error)
		default:
			labutil.PrintException("Unknown Command Type: " + cmd.Op)
			labutil.PanicSystem()
			return
		}
	}

	labutil.PrintDirect("\n======GetaKV Quit======\n")
	kvs.Kill()
}
