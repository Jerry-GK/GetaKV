package raft

import (
	"strconv"
	"time"

	"../common"
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
	rf.Lock()
	defer rf.Unlock()
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Receive> RPC_CALLEE_RequestVote from Server[" + strconv.Itoa(args.CandidateId) + "]")

	//default reply: not granted
	reply.Term = rf.term
	reply.VoteGranted = false

	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLastLogTerm()

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
			if rf.voteFor == -1 {
				//first vote
				//election restriction
				if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
					//log condition not satisfied
					return
				}
				rf.voteFor = args.CandidateId
				reply.VoteGranted = true
				rf.ResetElectionTimer() // reset election timer when first vote for a candidate
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
		rf.ChangeState(Follower, args.Term) //reset election timer in ChangeState
		//try to first vote imeediately
		//election restriction
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
			//log condition not satisfied
			return
		}
		rf.voteFor = args.CandidateId
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
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Send> RPC_CALLER_SendRequestVote to Server[" + strconv.Itoa(peerIdx) + "]")

	rpcBacthTimer := time.NewTimer(RPCBatchTimeout)
	defer rpcBacthTimer.Stop()

	rpcSingleTimer := time.NewTimer(RPCSingleTimeout)
	defer rpcSingleTimer.Stop()

	for {
		rf.Lock()
		if rf.state != Candidate {
			rf.Unlock()
			return false
		}
		rf.Unlock()

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
			rf.Lock()
			peer := rf.peers[peerIdx]
			rf.Unlock()
			//no lock for parallel RPC call
			//RPC_RV_TotalCallNum++
			//RPC_ConcurrentCallNum++
			ok := peer.Call("Raft.RPC_CALLEE_RequestVote", args, &rTemp)
			//RPC_ConcurrentCallNum--
			ch <- ok
		}()

		select {
		case <-rpcSingleTimer.C:
			//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-RequestVote Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-RequestVote Time Out Quit")
			return false
		case ok := <-ch:
			if !ok {
				//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Error> RPC_CALLER_SendRequestVote to Server[" + strconv.Itoa(peerIdx) + "], Retry")
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

func (rf *Raft) StartElection() {
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: StartElection")
	//rf.ChangeState(Candidate) has been called
	//has an outer lock
	// rf.Lock()
	// defer rf.Unlock()

	rf.Lock()

	//parallel request vote from all peers
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm:  rf.GetLastLogTerm(),
	}

	//vote for self
	rf.voteFor = rf.me
	rf.Unlock()

	voteGrantedCount := 1

	votesCh := make(chan bool, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(ch chan bool, i int) {
				var reply RequestVoteReply
				rf.Lock()
				if rf.state != Candidate {
					rf.Unlock()
					ch <- reply.VoteGranted
					return
				}
				rf.Unlock()

				ok := rf.RPC_CALLER_SendRequestVote(i, args, &reply) //no need to lock (parallel)
				if !ok {
					ch <- false
					return
				}

				rf.Lock()
				if reply.Term > rf.term {
					//candidate should always give up election if receive RPC with higher term

					rf.ChangeState(Follower, reply.Term)
					rf.Unlock()
					ch <- reply.VoteGranted
					return
				}
				rf.Unlock()

				ch <- reply.VoteGranted
			}(votesCh, i)
		}
	}

	voteFromOthers := 0
	for {
		select {
		case g := <-votesCh: //get vote
			//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: get vote " + strconv.FormatBool(g))
			voteFromOthers++
			rf.Lock()
			flag := g && rf.state == Candidate

			if flag {
				voteGrantedCount++
				//leader condition: majority votes (more than half)
				if voteGrantedCount > len(rf.peers)/2 {
					//no need to wait for all votes if already able to become leader
					rf.ChangeState(Leader, rf.term)
					rf.Unlock()
					return
				}
			}
			if voteFromOthers >= len(rf.peers)-1 {
				if rf.state != Leader {
					//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: lose election")
					//rf.ChangeState(Follower, rf.term - 1) //is that right?
					rf.ChangeState(Follower, rf.term)
				}
				rf.Unlock()
				return
			}
			rf.Unlock()

		case <-rf.electionTimer.C: //election timeout, become follower
			rf.Lock()
			if rf.state == Candidate {
				//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: election timeout")
				//do not decrease term!
				rf.ChangeState(Follower, rf.term)
				rf.Unlock()
				return
			} else {
				//common.PrintWarning("Server[" + strconv.Itoa(rf.me) + "]: election timeout, but not candidate")
				//may be normal?
			}
			rf.Unlock()
		}
	}

	return //should never reach here
}
