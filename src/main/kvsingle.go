package main

import (
	"../kvraft"
	"../labrpc"
	"../labutil"
)

type Command struct {
	Op    string
	Key   string
	Value string
}

func Parse(input string) Command {
	//parse to be implemented
	cmd := Command{
		Op:    "GET",
		Key:   "0",
		Value: "",
	}
	return cmd
}

func main() {
	//make service
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

	//make single client
	ck := kvraft.MakeClerk(ends)

	quit := false

	for !quit {
		//input command
		input := "GET 0"
		cmd := Parse(input)

		switch cmd.Op {
		case "PUT":
			ck.Put(cmd.Key, cmd.Value)
			labutil.PrintMessage("Put Success")
		case "APPEND":
			ck.Append(cmd.Key, cmd.Value)
			labutil.PrintMessage("Append Success")
		case "GET":
			value := ck.Get(cmd.Key)
			labutil.PrintMessage("Get Success, value = " + value)
		case "QUIT":
			quit = true
			break
		default:
			labutil.PrintException("Unknown command type: " + cmd.Op)
			labutil.PanicSystem()
			return
		}
	}

	labutil.PrintMessage("Client Quit")
}
