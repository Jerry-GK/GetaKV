package raft

import (
	"fmt"
	"time"

	"../labutil"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RPC_CALLEE_RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//RPC receiver function is locked
	rf.lock()
	defer rf.unlock()
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Receive> RPC_CALLEE_RequestVote from Server[" + fmt.Sprint(args.CandidateId) + "]")

	//default reply: not granted
	reply.Term = rf.term
	reply.VoteGranted = false

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.state == Leader {
			//Leader refuse to vote
			return
		} else if rf.state == Candidate {
			//Other Candidate refuse to vote
			return
		} else if rf.state == Follower {
			if rf.voteFor == InvalidVoteFor {
				//first vote
				//election restriction
				if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
					//log condition not satisfied
					return
				}
				rf.setVoteFor(args.CandidateId)
				reply.VoteGranted = true
				rf.resetElectionTimer() // reset election timer when first vote for a candidate
				return
			} else if rf.voteFor == args.CandidateId {
				//has voted for this candidate
				reply.VoteGranted = true
				return
			} else {
				//has voted for other candidate
				return
			}
		}
	} else if args.Term > rf.term {
		//issue: should a server update term and change to Follower when receiving RequestVote RPC
		//from (newly changed) candidate?
		//espcially for the leader who only lost one APPENDENTRIES RPC to the candidate
		//ans: Yes(candidare first)

		//issue: really need to vote for the candidate? or just change to Follower?
		//ans: vote immediately

		//become Follower and vote for the candidate immediately
		rf.changeState(Follower, args.Term) //reset election timer in changeState
		//try to first vote imeediately
		//election restriction
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
			//log condition not satisfied
			return
		}
		rf.setVoteFor(args.CandidateId)
		reply.VoteGranted = true
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) RPC_CALLER_SendRequestVote(peerIdx int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//no RPC timeout yet
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Send> RPC_CALLER_SendRequestVote to Server[" + fmt.Sprint(peerIdx) + "]")

	rpcBacthTimer := time.NewTimer(RPCBatchTimeout)
	defer rpcBacthTimer.Stop()

	rpcSingleTimer := time.NewTimer(RPCSingleTimeout)
	defer rpcSingleTimer.Stop()

	for {
		rf.lock()
		if rf.state != Candidate {
			rf.unlock()
			return false
		}
		rf.unlock()

		rpcSingleTimer.Stop()
		rpcSingleTimer.Reset(RPCSingleTimeout)

		ch := make(chan bool, 1)
		rTemp := RequestVoteReply{}

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
			//RPC_RV_TotalCallNum++
			//RPC_ConcurrentCallNum++
			ok := peer.Call("Raft.RPC_CALLEE_RequestVote", args, &rTemp)
			//RPC_ConcurrentCallNum--
			ch <- ok
		}()

		select {
		case <-rf.stopCh:
			return false
		case <-rpcSingleTimer.C:
			//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: RPC-RequestVote Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: RPC-RequestVote Time Out Quit")
			return false
		case ok := <-ch:
			if !ok {
				//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: <RPC-Error> RPC_CALLER_SendRequestVote to Server[" + fmt.Sprint(peerIdx) + "], Retry")
				//sleep for a short time to avoid busy loop if RPC call fails immediately
				time.Sleep(RPCInterval)
				continue
				//continue
			} else {
				reply.Term = rTemp.Term
				reply.VoteGranted = rTemp.VoteGranted
				return ok
			}
		}
	}
	return false //should never reach here
}

func (rf *Raft) startElection() {
	labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: startElection")
	//rf.changeState(Candidate) has been called
	//has an outer lock
	// rf.lock()
	// defer rf.unlock()

	rf.lock()

	//parallel request vote from all peers
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	//vote for self
	rf.setVoteFor(rf.me)
	rf.unlock()

	voteGrantedCount := 1

	votesCh := make(chan bool, len(rf.peers))

	for ii := 0; ii < len(rf.peers); ii++ {
		if ii != rf.me {
			go func(ch chan bool, i int) {
				var reply RequestVoteReply
				rf.lock()
				if rf.state != Candidate {
					rf.unlock()
					ch <- reply.VoteGranted
					return
				}
				rf.unlock()

				ok := rf.RPC_CALLER_SendRequestVote(i, args, &reply) //no need to lock (parallel)
				if !ok {
					ch <- false
					//RPC Caller failed should be treated as RequestVote failed, and not retry
					//issue: maybe should written in a loop(for !killed()) and retry?
					return
				}

				//rf.term, rf.state may be different after RV!

				rf.lock()

				if reply.Term > rf.term {
					//candidate should always give up election if receive RPC with higher term
					rf.changeState(Follower, reply.Term)
				}

				rf.unlock()
				ch <- reply.VoteGranted
			}(votesCh, ii)
		}
	}

	voteFromOthers := 0
	for {
		select {
		case <-rf.stopCh:
			return
		case g := <-votesCh: //get vote
			//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: get vote " + fmt.Sprint(g))
			voteFromOthers++
			rf.lock()
			flag := g && rf.state == Candidate

			if flag {
				voteGrantedCount++
				//leader condition: majority votes (more than half)
				if voteGrantedCount > len(rf.peers)/2 {
					//no need to wait for all votes if already able to become leader
					rf.changeState(Leader, rf.term)
					rf.unlock()
					return
				}
			}
			if voteFromOthers >= len(rf.peers)-1 {
				if rf.state != Leader {
					//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: lose election")
					//rf.changeState(Follower, rf.term - 1) //is that right?
					rf.changeState(Follower, rf.term)
				}
				rf.unlock()
				return
			}
			rf.unlock()

		case <-rf.electionTimer.C: //election timeout, become follower
			rf.lock()
			if rf.state == Candidate {
				//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: election timeout")
				//do not decrease term!
				rf.changeState(Follower, rf.term)
				rf.unlock()
				return
			} else {
				//labutil.PrintWarning("Server[" + fmt.Sprint(rf.me) + "]: election timeout, but not candidate")
				//issue: is that normal?
			}
			rf.unlock()
		}
	}

	return //should never reach here
}
