package labutil

import (
	"fmt"
	"time"
)

func PauseSystem(seconds int) {
	fmt.Println("[System]: Pause for " + fmt.Sprint(seconds) + " seconds...")
	time.Sleep(time.Duration(seconds) * time.Second)
}

func PanicSystem() {
	panic("[Panic]: System Stopped!")
}

func PrintMessage(msg string) {
	fmt.Println("[Message]: " + msg)
}
