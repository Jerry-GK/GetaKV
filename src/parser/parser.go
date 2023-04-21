package parser

import "strings"

type Command struct {
	Op    string
	Key   string
	Value string
	Error string
}

type Parser struct {
	//no members yet
}

func (p *Parser) Parse(input string) Command {
	//parse to be implemented
	//exampe:
	// <"GET 0">, <"PUT 0 1">, <"APPEND 0 1">, <"QUIT">

	cmd := Command{
		Op:    "INVALID",
		Key:   "",
		Value: "",
		Error: "Empty Command",
	}

	ss := strings.Fields(input) //separate by space
	len := len(ss)
	if len == 0 {
		return cmd
	}

	op := strings.ToUpper(ss[0])

	switch op {
	case "GET":
		if len == 2 {
			cmd = Command{
				Op:    "GET",
				Key:   ss[1],
				Value: "",
			}
		} else {
			cmd.Error = "GET should have exactly 1 argument (spaces in string not available)"
		}
	case "PUT":
		if len == 3 {
			cmd = Command{
				Op:    "PUT",
				Key:   ss[1],
				Value: ss[2],
			}
		} else {
			cmd.Error = "PUT should have exactly 2 arguments (spaces in string not available)"
		}
	case "APPEND":
		if len == 3 {
			cmd = Command{
				Op:    "APPEND",
				Key:   ss[1],
				Value: ss[2],
			}
		} else {
			cmd.Error = "APPEND should have exactly 2 arguments (spaces in string not available)"
		}
	case "QUIT":
		if len == 1 {
			cmd = Command{
				Op:    "QUIT",
				Key:   "",
				Value: "",
			}
		} else {
			cmd.Error = "Invalid QUIT Command"
		}
	default:
		cmd.Error = "Unknown Operation: " + op
	}

	return cmd
}

func MakeParser() *Parser {
	return &Parser{}
}
