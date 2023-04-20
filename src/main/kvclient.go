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
	ends := make([]*labrpc.ClientEnd, 5) //should come from kvservice

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
