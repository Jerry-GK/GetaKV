package raft

import (
	"fmt"
	"time"

	"../labutil"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//Log entries to send during APPENDENTRIES RPC is generated here
//must have outer lock!
func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogIdx := rf.getLastLogIndex()
	//default empty log for heartbeat
	prevLogIndex := lastLogIdx
	prevLogTerm := rf.getLastLogTerm()
	entries := make([]LogEntry, 0)

	if nextIdx > lastLogIdx || nextIdx < rf.lastIncludedIndex {
		//no log to send
		//send empty entries
		prevLogIndex = lastLogIdx
		prevLogTerm = rf.getLastLogTerm()
	} else {
		//no-empty log to send
		prevLogIndex = nextIdx - 1
		if prevLogIndex == 0 && rf.lastIncludedIndex == 0 { //first log entry
			prevLogTerm = InvalidTerm
		} else if prevLogIndex < rf.lastIncludedIndex {
			prevLogTerm = rf.lastIncludedTerm
		} else {
			prevLogTerm = rf.getLogEntryByIndex(prevLogIndex).Term
		}
		//may send more than one entries for efficiency
		entries = append(entries, rf.getLogEntriesByIndexRange(prevLogIndex+1, 0)...)
	}

	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	return args
}

func (rf *Raft) RPC_CALLEE_AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//RPC receiver function is locked
	rf.lock()
	defer rf.unlock()
	if len(args.Entries) > 0 {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Receive> RPC_CALLEE_AppendEntries from Server[" + fmt.Sprint(args.LeaderId) + "] (size = " + fmt.Sprint(len(args.Entries)) + ")")
	}

	//default reply: not success
	reply.Term = rf.term
	reply.Success = false

	//1. reply false if term < currentTerm
	if args.Term < rf.term {
		//issue: if a candidate receives an AppendEntries RPC from a leader with even a smaller term
		//does it need to convert to follower and decrease its term?
		//or just ignore the RPC?
		//ans: No(candidate first)

		//in this case, leader should step down without retrying smaller nextIndex
		return
	}

	//args.Term >= rf.term

	//Impossible situation: leader receives an AppendEntries RPC from a leader with the same term!
	if args.Term == rf.term && rf.state == Leader {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) +
			"]: Leader receives AppendEntries RPC from another Leader with the same term " + fmt.Sprint(rf.term))
		labutil.PanicSystem()
		return
	}

	rf.changeState(Follower, args.Term) //reset election timer in changeState

	if len(args.Entries) != 0 {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Try to append log entries from leader RPC")
	}

	//replicate log entries
	if args.PrevLogIndex+1 < rf.lastIncludedIndex {
		//return reply.Success = false
		return
	}
	//2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	//this condition is very important
	if rf.getLastLogIndex() < args.PrevLogIndex ||
		(args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogTerm != InvalidTerm &&
			args.PrevLogIndex > 0 && rf.getLogEntryByIndex(args.PrevLogIndex).Term != args.PrevLogTerm) {
		return
	}
	//3. if an existing entry conflicts with a new one (same index but different terms)
	//this is one of the most tricky part!
	index := 0
	max_i_InLog := -1
	for i := 0; i < len(args.Entries); i++ {
		index = args.PrevLogIndex + 1 + i
		//condition index > 0 is not necessary
		if index > 0 && rf.getLastLogIndex() >= index && rf.getLogEntryByIndex(index).Term != args.Entries[i].Term {
			//delete all entries after index (truncate)
			labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Truncate logEntries of index range [1, " + fmt.Sprint(index) + "), last commit index = " + fmt.Sprint(rf.commitIndex))

			//rf.logEntries = rf.logEntries[:index-1]
			start := 1
			if rf.lastIncludedIndex > 0 {
				start = rf.lastIncludedIndex
			}
			rf.setLogEntries(rf.getLogEntriesByIndexRange(start, index))
			break
		} else if index > 0 && rf.getLastLogIndex() >= index && rf.getLogEntryByIndex(index).Term == args.Entries[i].Term {
			max_i_InLog = i
		}
	}

	//4. append any new entries <not already> in the log, don't forget to slice args.Entries!
	//rf.logEntries = append(rf.logEntries, args.Entries[max_i_InLog+1:len(args.Entries)]...)
	rf.appendLogEntries(args.Entries[max_i_InLog+1 : len(args.Entries)]...)

	//5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	//piggyback commitIndex here!
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := labutil.MinOfInt(args.LeaderCommit, rf.getLastLogIndex())
		//Non-Leader commit log entries
		rf.setCommitIndex(newCommitIndex)
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Non-Leader commitIndex updated to " + fmt.Sprint(rf.commitIndex))
	}

	if len(args.Entries) != 0 {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Append log entries from leader RPC success")
	}

	reply.Success = true
	return
}

func (rf *Raft) RPC_CALLER_AppendEntriesToPeer(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//no RPC timeout yet
	if len(args.Entries) > 0 {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Send> RPC_CALLER_AppendEntriesToPeer to Server[" + fmt.Sprint(peerIdx) + "] (size = " + fmt.Sprint(len(args.Entries)) + ")")
	}

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
		rTemp := AppendEntriesReply{}

		go func() {
			//RPC call may
			//1. return immediately (ok ==  true or ok == false, false may cause busy loop)
			//2. return after a short time (ok == true or ok == false)
			//3. return after a long time (single RPC call timeout, retry, should ignore the reply)
			//4. never return (single RPC call timeout, retry, no reply)
			//give up if batch RPC call timeout after several retries
			//RPC reply may come in any order
			rf.lock()
			peer := rf.peers[peerIdx]
			rf.unlock()
			//no lock for parallel RPC call
			//RPC_AE_TotalCallNum++
			//RPC_ConcurrentCallNum++
			ok := peer.Call("Raft.RPC_CALLEE_AppendEntries", args, &rTemp)
			//RPC_ConcurrentCallNum--
			ch <- ok
		}()

		select {
		case <-rf.stopCh:
			return false
		case <-rpcSingleTimer.C:
			//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: RPC-AppendEntries Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: RPC-AppendEntries Time Out Quit")
			return false
			//continue //retry indefinitely
		case ok := <-ch:
			if !ok {
				//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Error> rpc_CALLER_AppendEntriesToPeer to Server[" + fmt.Sprint(peerIdx) + "] failed, Retry")
				//sleep for a short time to avoid busy loop if RPC call fails immediately
				time.Sleep(RPCInterval)
				continue
			} else {
				//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: RPC-AppendEntries Not Time Out, Success")
				reply.Term = rTemp.Term
				reply.Success = rTemp.Success
				return ok
			}
		}
	}
	return false //should never reach here
}

func (rf *Raft) startHeartBeat() {
	//parallel append empty enyries to all followers
	labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: startHeartBeat")
	// rf.lock()
	// defer rf.unlock()

	for ii := 0; ii < len(rf.peers); ii++ {
		rf.lock()
		if rf.state != Leader {
			rf.unlock()
			return
		}
		rf.unlock()
		if ii != rf.me {
			go func(i int) {
				//try different nextIndex[i]
				for !rf.killed() {
					rf.lock()
					//get args to append
					//may have empty entry for heartbeat checking or non-empty entries for log replication
					if rf.state != Leader {
						rf.unlock()
						return
					}

					flag := rf.nextIndex[i] < rf.lastIncludedIndex
					rf.unlock()
					if flag {
						go rf.sendInstallSnapshot(i)
						return // give up AE this time
					}

					rf.lock()
					args := rf.getAppendEntriesArgs(i)
					reply := AppendEntriesReply{}
					rf.unlock()

					//no need to lock (parallel)
					//have internal check for state == Leader
					ok := rf.RPC_CALLER_AppendEntriesToPeer(i, &args, &reply)
					if !ok {
						//issue: try indifinitely or give up if RPC(Caller) call fails?
						//ans: give up. retry in next heartbeat AE
						return
						//retry to AE forever until success or not current leader anymore
						//continue
					}

					//rf.term, rf.state may be different after AE!

					rf.lock()
					if reply.Term > rf.term {
						//issue: should the leader update term and change to follower
						//even receiving from APPENDENTRIES RPC reply from the new candidate?
						//ans: Yes(candidate first)
						rf.changeState(Follower, reply.Term)
					}

					//isssue: does the server need to response to AE reply if it is not current leader?
					//ans: No
					if rf.state != Leader || rf.term != args.Term {
						rf.unlock()
						return
					}

					if reply.Success {
						//update nextIndex and matchIndex for follower
						rf.nextIndex[i] = args.PrevLogIndex + 1 + len(args.Entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1

						//update commitIndex if condition satisfied (called even for appending empty entries)
						rf.updateLeaderCommitIndex()
						rf.unlock()
						return
					} else {
						//decrease nextIndex and retry (slow backup)
						if rf.nextIndex[i] <= 1 {
							rf.unlock()
							//every try fails
							//issue: is that normal?
							labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Try AE for every nextIndex all failed!")
							//issue: need to send snapshot?
							return
						}

						//No fast backup
						rf.nextIndex[i]--
						//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: Retry AE to Server[" + fmt.Sprint(i) + "], nextIndex = " + fmt.Sprint(rf.nextIndex[i]))
						rf.unlock()
						continue
					}

				}
				return //should never reach here
			}(ii)
		}
	}
}

//must have outer lock
func (rf *Raft) updateLeaderCommitIndex() {
	//if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	//and log[N].term == currentTerm:
	//set commitIndex = N
	labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Try to updateLeaderCommitIndex")

	//labutil.PrintDebug("rf.commitIndex = " + fmt.Sprint(rf.commitIndex) + ", rf.getLastLogIndex() = " + fmt.Sprint(rf.getLastLogIndex()))
	updated := false

	//checking N from large to small
	//issue: which is better, large to small or small to large?
	//for N := rf.getLastLogIndex(); N >= rf.commitIndex+1; N-- {
	for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
		//check if log[N].term == currentTerm
		//labutil.PrintDebug("Checking N = " + fmt.Sprint(N))
		if rf.lastIncludedIndex != 0 && N-rf.lastIncludedIndex+1 < 0 {
			labutil.PrintWarning("Strange: commitIndex = " + fmt.Sprint(rf.commitIndex) + ", lastIncludedIndex = " + fmt.Sprint(rf.lastIncludedIndex) + ", N = " + fmt.Sprint(N))
			continue
		}
		if rf.getLogEntryByIndex(N).Term != rf.term {
			//only commit log entries of current term!
			continue
		}
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N {
				count++
				if count > len(rf.peers)/2 {
					//Leader commit log entries
					rf.setCommitIndex(N)
					labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Leader commitIndex updated to " + fmt.Sprint(rf.commitIndex))
					updated = true
					break
				}
			}
		}
		if updated {
			break //no need to check larger N
		}
	}
	if !updated {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: updateLeaderCommitIndex Failed")
	}
}
