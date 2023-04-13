package raft

import (
	"strconv"
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
func (rf *Raft) GetAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogIdx := rf.GetLastLogIndex()
	//default empty log for heartbeat
	prevLogIndex := lastLogIdx
	prevLogTerm := rf.GetLastLogTerm()
	entries := make([]LogEntry, 0)

	if nextIdx > lastLogIdx {
		//no log to send
		//send empty entries
	} else {
		//no-empty log to send
		prevLogIndex = nextIdx - 1
		if prevLogIndex <= 0 { //first log entry
			prevLogTerm = InvalidTerm
		} else {
			prevLogTerm = rf.GetLogEntryByIndex(prevLogIndex).Term
		}
		//may send more than one entries for efficiency
		//entries = append(entries, rf.logEntries[nextIdx-1:]...)
		entries = append(entries, rf.GetLogEntriesByIndexRange(prevLogIndex+1, 0)...)
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
	rf.Lock()
	defer rf.Unlock()
	if len(args.Entries) > 0 {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Receive> RPC_CALLEE_AppendEntries from Server[" + strconv.Itoa(args.LeaderId) + "] (size = " + strconv.Itoa(len(args.Entries)) + ")")
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
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) +
			"]: Leader receives AppendEntries RPC from another Leader with the same term " + strconv.Itoa(rf.term))
		labutil.PanicSystem()
		return
	}

	rf.ChangeState(Follower, args.Term) //reset election timer in ChangeState

	if len(args.Entries) != 0 {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Try to append log entries from leader RPC")
	}

	//replicate log entries
	//2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.GetLastLogIndex() < args.PrevLogIndex ||
		(args.PrevLogIndex != 0 && args.PrevLogTerm != InvalidTerm &&
			rf.GetLogEntryByIndex(args.PrevLogIndex).Term != args.PrevLogTerm) {
		return
	}
	//3. if an existing entry conflicts with a new one (same index but different terms)
	//this is one of the most tricky part!
	index := 0
	max_i_InLog := -1
	for i := 0; i < len(args.Entries); i++ {
		index = args.PrevLogIndex + 1 + i
		//condition index > 0 is not necessary
		if index > 0 && rf.GetLastLogIndex() >= index && rf.GetLogEntryByIndex(index).Term != args.Entries[i].Term {
			//delete all entries after index (truncate)
			labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Truncate logEntries of index range [1, " + strconv.Itoa(index) + "), last commit index = " + strconv.Itoa(rf.commitIndex))

			//rf.logEntries = rf.logEntries[:index-1]
			rf.SetLogEntries(rf.GetLogEntriesByIndexRange(1, index))
			break
		} else if index > 0 && rf.GetLastLogIndex() >= index && rf.GetLogEntryByIndex(index).Term == args.Entries[i].Term {
			max_i_InLog = i
		}
	}

	//4. append any new entries <not already> in the log, don't forget to slice args.Entries!
	//rf.logEntries = append(rf.logEntries, args.Entries[max_i_InLog+1:len(args.Entries)]...)
	rf.AppendLogEntries(args.Entries[max_i_InLog+1 : len(args.Entries)]...)

	//5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	//piggyback commitIndex here!
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := labutil.MinOfInt(args.LeaderCommit, rf.GetLastLogIndex())
		//Non-Leader commit log entries
		rf.SetCommitIndex(newCommitIndex)
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Non-Leader commitIndex updated to " + strconv.Itoa(rf.commitIndex))
	}

	if len(args.Entries) != 0 {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Append log entries from leader RPC success")
	}

	reply.Success = true
	return
}

func (rf *Raft) RPC_CALLER_AppendEntriesToPeer(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//no RPC timeout yet
	if len(args.Entries) > 0 {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Send> RPC_CALLER_AppendEntriesToPeer to Server[" + strconv.Itoa(peerIdx) + "] (size = " + strconv.Itoa(len(args.Entries)) + ")")
	}

	rpcBacthTimer := time.NewTimer(RPCBatchTimeout)
	defer rpcBacthTimer.Stop()

	rpcSingleTimer := time.NewTimer(RPCSingleTimeout)
	defer rpcSingleTimer.Stop()

	for {
		rf.Lock()
		if rf.state != Leader {
			rf.Unlock()
			return false
		}
		rf.Unlock()

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
			rf.Lock()
			peer := rf.peers[peerIdx]
			rf.Unlock()
			//no lock for parallel RPC call
			//RPC_AE_TotalCallNum++
			//RPC_ConcurrentCallNum++
			ok := peer.Call("Raft.RPC_CALLEE_AppendEntries", args, &rTemp)
			//RPC_ConcurrentCallNum--
			ch <- ok
		}()

		select {
		case <-rpcSingleTimer.C:
			//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-AppendEntries Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-AppendEntries Time Out Quit")
			return false
		case ok := <-ch:
			if !ok {
				//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Error> RPC_CALLER_AppendEntriesToPeer to Server[" + strconv.Itoa(peerIdx) + "] failed, Retry")
				//sleep for a short time to avoid busy loop if RPC call fails immediately
				time.Sleep(RPCInterval)
				continue
			} else {
				reply.Term = rTemp.Term
				reply.Success = rTemp.Success
				return ok
			}
		}
	}
	return false //should never reach here
}

func (rf *Raft) StartHeartBeatCheck() {
	//parallel append empty enyries to all followers
	labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: StartHeartBeatCheck")
	// rf.Lock()
	// defer rf.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.Lock()
		if rf.state != Leader {
			rf.Unlock()
			return
		}
		rf.Unlock()

		if i != rf.me {
			go func(i int) {
				//try different nextIndex[i]
				for !rf.killed() {
					rf.Lock()
					//get args to append
					//may have empty entry for heartbeat checking or non-empty entries for log replication
					args := rf.GetAppendEntriesArgs(i)

					reply := AppendEntriesReply{}

					if rf.state != Leader {
						rf.Unlock()
						return
					}
					rf.Unlock()

					//no need to lock (parallel)
					//have internal check for state == Leader
					ok := rf.RPC_CALLER_AppendEntriesToPeer(i, &args, &reply)
					if !ok {
						//issue: try indifinitely or give up if RPC(wrapper) call fails?
						//ans: give up. retry in next heartbeat AE
						//retry to AE forever until success or not current leader anymore
						//continue
						return
					}

					//rf.term, rf.state may be different after AE!

					rf.Lock()
					if reply.Term > rf.term {
						//issue: should the leader update term and change to follower
						//even receiving from APPENDENTRIES RPC reply from the new candidate?
						//ans: Yes(candidate first)
						rf.ChangeState(Follower, reply.Term)
					}

					//isssue: does the server need to response to AE reply if it is not current leader?
					//ans: No
					if rf.state != Leader || rf.term != args.Term {
						rf.Unlock()
						return
					}

					if reply.Success {
						//update nextIndex and matchIndex for follower
						rf.nextIndex[i] = args.PrevLogIndex + 1 + len(args.Entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1

						//update commitIndex if condition satisfied (called even for appending empty entries)
						rf.UpdateLeaderCommitIndex()
						rf.Unlock()
						return
					} else {
						//decrease nextIndex and retry (slow backup)
						if rf.nextIndex[i] <= 1 {
							rf.Unlock()
							//every try fails
							labutil.PrintWarning("Server[" + strconv.Itoa(rf.me) + "]: Try AE for every nextIndex all failed!")
							return
						}

						//No fast backup
						rf.nextIndex[i]--
						labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Retry AE to Server[" + strconv.Itoa(i) + "], nextIndex = " + strconv.Itoa(rf.nextIndex[i]))
						rf.Unlock()
						continue
					}

				}
				return //should never reach here
			}(i)
		}
	}
}

//must have outer lock
func (rf *Raft) UpdateLeaderCommitIndex() {
	//if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	//and log[N].term == currentTerm:
	//set commitIndex = N
	labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Try to UpdateLeaderCommitIndex")

	//labutil.PrintDebug("rf.commitIndex = " + strconv.Itoa(rf.commitIndex) + ", rf.GetLastLogIndex() = " + strconv.Itoa(rf.GetLastLogIndex()))
	updated := false

	//checking N from large to small
	//issue: which is better, large to small or small to large?
	//for N := rf.GetLastLogIndex(); N >= rf.commitIndex+1; N-- {
	for N := rf.commitIndex + 1; N <= rf.GetLastLogIndex(); N++ {
		//check if log[N].term == currentTerm
		//labutil.PrintDebug("Checking N = " + strconv.Itoa(N))
		if rf.GetLogEntryByIndex(N).Term != rf.term {
			//only commit log entries of current term!
			continue
		}
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N {
				count++
				if count > len(rf.peers)/2 {
					//Leader commit log entries
					rf.SetCommitIndex(N)
					labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Leader commitIndex updated to " + strconv.Itoa(rf.commitIndex))
					updated = true
					break
				}
			}
		}
		if updated {
			break //no need to check smaller N
		}
	}
	if !updated {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: UpdateLeaderCommitIndex Failed")
	}
}
