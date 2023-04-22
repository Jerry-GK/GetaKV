package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"os"
	"sync"
)

const (
	filesRoot = "../main/files/"
)

type Persister struct {
	mu               sync.Mutex
	raftstate        []byte
	snapshot         []byte
	me               int
	raftFileName     string
	snapshotFileName string
	toFile           bool
}

func MakePersister() *Persister {
	return &Persister{}
}

func MakePersisterWithFile(me int) *Persister {
	ps := new(Persister)
	ps.me = me
	ps.raftFileName = filesRoot + "Server-" + fmt.Sprint(ps.me) + ".rf"
	ps.snapshotFileName = filesRoot + "Server-" + fmt.Sprint(ps.me) + ".kv"
	ps.toFile = true

	if ps.toFile {
		//check if file exists
		_, errr := os.Stat(ps.raftFileName)
		_, errs := os.Stat(ps.snapshotFileName)
		if errr == nil && errs == nil {
			// file exists
			ps.readFile()
		}
	}
	return ps
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (ps *Persister) Close() {
	if ps.toFile {
		ps.writeFile()
	}
}

func (ps *Persister) writeFile() {
	fr, errr := os.Create(ps.raftFileName)
	defer fr.Close()
	if errr != nil {
		panic(errr)
	}
	_, errr = fr.Write(ps.raftstate)
	if errr != nil {
		panic(errr)
	}

	fs, errs := os.Create(ps.snapshotFileName)
	defer fs.Close()
	if errs != nil {
		panic(errs)
	}
	defer fs.Close()
	_, errs = fs.Write(ps.snapshot)
	if errs != nil {
		panic(errs)
	}
}

func (ps *Persister) readFile() {
	fr, errr := os.Open(ps.raftFileName)
	defer fr.Close()
	if errr != nil {
		panic(errr)
	}
	fir, errr := fr.Stat()
	if errr != nil {
		panic(errr)
	}
	ps.raftstate = make([]byte, fir.Size())
	_, errr = fr.Read(ps.raftstate)
	if errr != nil {
		panic(errr)
	}

	fs, errs := os.Open(ps.snapshotFileName)
	defer fs.Close()
	if errs != nil {
		panic(errs)
	}
	fis, errs := fs.Stat()
	if errs != nil {
		panic(errs)
	}
	ps.snapshot = make([]byte, fis.Size())
	_, errs = fs.Read(ps.snapshot)
	if errs != nil {
		panic(errs)
	}

}
