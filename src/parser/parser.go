package parser

import "strings"

type Command struct {
	Op    string
	Key   string
	Value string
}

func Parse(input string) Command {
	//parse to be implemented
	//exampe:
	// <"GET 0">, <"PUT 0 1">, <"APPEND 0 1">

	cmd := Command{
		Op:    "INVALID",
		Key:   "",
		Value: "",
	}

	ss := strings.Fields(input) //separate by space
	len := len(ss)
	if len == 0 {
		return cmd
	}

	ss[0] = strings.ToUpper(ss[0])

	if len == 1 {
		if ss[0] == "QUIT" {
			cmd = Command{
				Op:    "QUIT",
				Key:   "",
				Value: "",
			}
		}
	} else if len == 2 {
		if ss[0] == "GET" {
			cmd = Command{
				Op:    "GET",
				Key:   ss[1],
				Value: "",
			}
		}
	} else if len == 3 {
		if ss[0] == "PUT" {
			cmd = Command{
				Op:    "PUT",
				Key:   ss[1],
				Value: ss[2],
			}
		} else if ss[0] == "APPEND" {
			cmd = Command{
				Op:    "APPEND",
				Key:   ss[1],
				Value: ss[2],
			}
		}
	}

	return cmd
}
