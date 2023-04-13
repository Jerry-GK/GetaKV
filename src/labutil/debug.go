package labutil

import (
	"fmt"
)

const Enable_Debug = false

const Enable_Warning = true
const Enable_Exception = true

func PrintDebug(msg string) {
	if Enable_Debug {
		fmt.Println("[Debug]: " + msg)
	}
}

func PrintWarning(msg string) {
	if Enable_Warning == true {
		fmt.Println("[Warning]: " + msg)
	}
}

func PrintException(msg string) {
	if Enable_Exception == true {
		fmt.Println("[Exception]: " + msg)
	}
}
