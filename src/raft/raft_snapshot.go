package raft

import (
	"time"
	//"../labutil"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int    //so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Data              []byte //raw bytes of the snapshot chunk, starting at offset

	//chunk related
	Offset int  //byte offset where chunk is positioned in the snapshot file, not used yet
	Done   bool //true if this is the last chunk, not used yet
}

type InstallSnapshotReply struct {
	Term int //currentTerm, for leader to update itself
}

func (rf *Raft) RPC_CALLEE_InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock()
	defer rf.unlock()

	reply.Term = rf.term

	//reply immediately if term < currentTerm
	if args.Term < rf.term {
		return
	}

	rf.changeState(Follower, args.Term)

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	//snapshot
	startIndex := args.LastIncludedIndex //startIndex >= args.LastIncludedIndex >= 1
	if startIndex > rf.getLastLogIndex() {
		rf.setLogEntries(make([]LogEntry, 0))
	} else {
		rf.setLogEntries(rf.getLogEntriesByIndexRange(startIndex, 0))
	}

	rf.setLastIncludedIndex(args.LastIncludedIndex)
	rf.setLastIncludedTerm(args.LastIncludedTerm)

	//issue: is this right?
	//ans: No, cannot make kv server readPeristSnapshot here
	// rf.SetCommitIndex(labutil.MaxOfInt(rf.commitIndex, rf.lastIncludedIndex))
	// rf.lastAppliedIndex = labutil.MaxOfInt(rf.lastAppliedIndex, rf.lastIncludedIndex)

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
}

func (rf *Raft) RPC_CALLER_InstallSnapshot(peerIdx int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rpcBacthTimer := time.NewTimer(RPCBatchTimeout)
	defer rpcBacthTimer.Stop()

	rpcSingleTimer := time.NewTimer(RPCSingleTimeout)
	defer rpcSingleTimer.Stop()

	for {
		rf.lock()
		if rf.state != Leader {
			rf.unlock()
			return false
		}
		rf.unlock()

		rpcSingleTimer.Stop()
		rpcSingleTimer.Reset(RPCSingleTimeout)

		ch := make(chan bool, 1)
		rTemp := RequestVoteReply{}

		go func() {
			rf.lock()
			peer := rf.peers[peerIdx]
			rf.unlock()
			//RPC call may
			//1. return immediately (ok ==  true or ok == false, false may cause busy loop)
			//2. return after a short time (ok == true or ok == false)
			//3. return after a long time (single RPC call timeout, retry, should ignore the reply)
			//4. never return (single RPC call timeout, retry, no reply)
			//give up if batch RPC call timeout after several retries
			//RPC reply may come in any order
			ok := peer.Call("Raft.RPC_CALLEE_InstallSnapshot", args, &rTemp)
			ch <- ok
			time.Sleep(time.Millisecond * 100)
		}()

		select {
		case <-rf.stopCh:
			return false
		case <-rpcSingleTimer.C:
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			return false
			//continue //retry indefinitely
		case ok := <-ch:
			if !ok {
				//sleep for a short time to avoid busy loop if RPC call fails immediately
				time.Sleep(RPCInterval)
				continue
			} else {
				reply.Term = rTemp.Term
				return ok
			}
		}
	}
	return false //should never reach here
}

func (rf *Raft) sendInstallSnapshot(peerIdx int) {
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: InstallSnapshot Call to " + fmt.Sprint(peerIdx))
	rf.lock()
	args := InstallSnapshotArgs{
		Term:              rf.term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),

		Offset: 0,    //not used yet
		Done:   true, //not used yet
	}
	reply := InstallSnapshotReply{}
	if rf.state != Leader {
		rf.unlock()
		return
	}
	rf.unlock()

	for !rf.killed() { //!killed() is necessary!
		ok := rf.RPC_CALLER_InstallSnapshot(peerIdx, &args, &reply) //may block here!
		if !ok {
			//no retry if install snapshot RPC caller fails
			//return
			//retry indefinitely
			continue
		} else {
			break
		}
	}

	rf.lock()
	defer rf.unlock()

	if reply.Term > rf.term {
		rf.changeState(Follower, reply.Term)
		return
	}

	if rf.state != Leader || rf.term != args.Term {
		return
	}

	if rf.nextIndex[peerIdx] < args.LastIncludedIndex+1 {
		rf.nextIndex[peerIdx] = args.LastIncludedIndex + 1
	}
	if rf.matchIndex[peerIdx] < args.LastIncludedIndex {
		rf.matchIndex[peerIdx] = args.LastIncludedIndex
	}
}
