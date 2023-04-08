package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../common"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	ElectionTimeout  = time.Millisecond * 300 // election(both election-check interval and election timeout)
	HeartBeatTimeout = time.Millisecond * 150 // leader heartbeat
	ApplyInterval    = time.Millisecond * 100 // apply log
	RPCSingleTimeout = time.Millisecond * 100 // may cause too long time to wait for RPC response if too big
	RPCBatchTimeout  = RPCSingleTimeout * 3   // may cause too many goroutines for RPC calls if too big
	RPCInterval      = time.Millisecond * 20  // may cause busy loop for RPC retry if too small
)

type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

func StateToString(state State) string {
	switch state {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	}
	return "Unknown"
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term       int
	state      State
	logEntries []LogEntry
	voteFor    int //-1 if not voted yet

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	stopCh chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.term
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
func (rf *Raft) RPCHANDLE_RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//RPC receiver function is locked
	rf.Lock()
	defer rf.Unlock()
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Receive> RPCHANDLE_RequestVote from Server[" + strconv.Itoa(args.CandidateId) + "]")

	//default reply: not granted
	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.state == Leader {
			//Leader always refuse to vote
			return
		} else if rf.state == Candidate {
			//Other Candidate always refuse to vote
			return
		} else if rf.state == Follower {
			if rf.voteFor == -1 {
				//first vote
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
func (rf *Raft) sendRequestVote(peerIdx int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//no RPC timeout yet
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Send> sendRequestVote to Server[" + strconv.Itoa(peerIdx) + "]")

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
			ok := peer.Call("Raft.RPCHANDLE_RequestVote", args, &rTemp)
			ch <- ok
		}()

		select {
		case <-rpcSingleTimer.C:
			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-RequestVote Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-RequestVote Time Out Quit")
			return false
		case ok := <-ch:
			if !ok {
				common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Error> sendRequestVote to Server[" + strconv.Itoa(peerIdx) + "], Retry")
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
		LastLogIndex: 0, //no need in lab2A
		LastLogTerm:  0, //no need in lab2A
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

				ok := rf.sendRequestVote(i, args, &reply) //no need to lock (parallel)
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
			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: get vote " + strconv.FormatBool(g))
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
					common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: lose election")
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
				common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: election timeout")
				//issue: does candidate need to decrease term if lose in election?
				//ans: No?
				//rf.ChangeState(Follower, rf.term - 1) //is that right?
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RPCHANDLE_AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//RPC receiver function is locked
	rf.Lock()
	defer rf.Unlock()
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Receive> RPCHANDLE_AppendEntries from Server[" + strconv.Itoa(args.LeaderId) + "]")

	//default reply: not success
	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term {
		//issue: if a candidate receives an AppendEntries RPC from a leader with even a smaller term
		//does it need to convert to follower and decrease its term?
		//or just ignore the RPC?
		//ans: No(candidate first)
		return
	} else if args.Term == rf.term {
		reply.Success = true
		if rf.state == Leader {
			//issue: is that possible that a leader receives an AppendEntries RPC from a leader with the same term?
			//ans: Impossible in Raft?!
			common.PrintException("Server[" + strconv.Itoa(rf.me) +
				"]: Leader receives AppendEntries RPC from another Leader with the same term " + strconv.Itoa(rf.term))
			time.Sleep(1000000 * time.Millisecond)
			return
		} else if rf.state == Candidate {
			//issue: will a candidate change to follower if it receives an AppendEntries RPC from a leader with the same term?
			//ans: Yes?
			rf.ChangeState(Follower, rf.term)
			return
		} else if rf.state == Follower {
			rf.ChangeState(Follower, rf.term)
			return
		}
	} else if args.Term > rf.term {
		reply.Success = true
		if rf.state == Leader {
			//issue: if a leader receives an AppendEntries RPC from a leader with a smaller term
			//for example: a leader recovers from failure and receives an AppendEntries RPC from the new leader
			//what will happen?
			//ans: the leader should convert to follower and decrease its term
			rf.ChangeState(Follower, args.Term)
			return
		} else if rf.state == Candidate {
			//issue: will a candidate change to follower
			//if it receives an AppendEntries RPC from a leader with a larger term?
			//ans: Yes
			rf.ChangeState(Follower, args.Term)
			return
		} else if rf.state == Follower {
			rf.ChangeState(Follower, args.Term)
			return
		}
	}

	//apply, no need to really apply in lab2A
}

func (rf *Raft) AppendEntriesToPeer(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//no RPC timeout yet
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Send> AppendEntriedToPeer to Server[" + strconv.Itoa(peerIdx) + "]")

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
			rf.Lock()
			peer := rf.peers[peerIdx]
			rf.Unlock()
			//no lock for parallel RPC call
			ok := peer.Call("Raft.RPCHANDLE_AppendEntries", args, &rTemp)
			ch <- ok
		}()

		select {
		case <-rpcSingleTimer.C:
			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-AppendEntries Time Out Retry")
			// retry single RPC call
			continue
		case <-rpcBacthTimer.C:
			common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: RPC-AppendEntries Time Out Quit")
			return false
		case ok := <-ch:
			if !ok {
				common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <RPC-Error> AppendEntriedToPeer to Server[" + strconv.Itoa(peerIdx) + "] failed, Retry")
				//sleep for a short time to avoid busy loop if RPC call fails immediately
				time.Sleep(RPCInterval)
				continue
				//continue
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
	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: StartHeartBeatCheck")
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
				rf.Lock()
				args := AppendEntriesArgs{rf.term, rf.me, 0, 0, make([]LogEntry, 0), 0}
				reply := AppendEntriesReply{}

				if rf.state != Leader {
					rf.Unlock()
					return
				}
				rf.Unlock()

				//no need to lock (parallel)
				//have internal check for state == Leader
				ok := rf.AppendEntriesToPeer(i, &args, &reply)
				if !ok {
					return
				}

				rf.Lock()
				if reply.Term > rf.term {
					//issue: should the leader update term and change to follower
					//even receiving from APPENDENTRIES RPC reply from the new candidate?
					//ans: Yes(candidate first)
					rf.ChangeState(Follower, reply.Term)
				}
				rf.Unlock()
				//Leader will ignore the reply with term <= its own term
			}(i)
		}
	}

	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ResetElectionTimer() {
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " reset election timer")
	rf.electionTimer.Stop()
	// add random time to avoid all boom at the same time
	r := time.Duration(rand.Int63()) % ElectionTimeout
	rf.electionTimer.Reset(ElectionTimeout + r)
}

func (rf *Raft) ResetHeartBeatTimer() {
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " reset heartbeat timer")
	rf.heartBeatTimer.Stop()
	// add random time to avoid all boom at the same time
	r := time.Duration(rand.Int63()) % HeartBeatTimeout
	rf.heartBeatTimer.Reset(HeartBeatTimeout + r)
}

func (rf *Raft) HitHeartBeatTimer() {
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " clear heartbeat timer")
	rf.heartBeatTimer.Stop()
	//almost elapsed immediately
	rf.heartBeatTimer.Reset(HeartBeatTimeout / 150)
}

//change the state of the server, update the term
//must have outer lock!
func (rf *Raft) ChangeState(state State, term int) {
	//reset election timer when changing state
	rf.ResetElectionTimer()

	// enable change to the same state
	// if state == rf.state {
	// 	common.PrintWarning("Server[" + strconv.Itoa(rf.me) + "]: " +
	//	StateToString(rf.state) + " changing to the same state!")
	// 	return
	// }

	common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: <State> " + StateToString(rf.state) + " -> " + StateToString(state) + " | <Term> " + strconv.Itoa(rf.term) + " -> " + strconv.Itoa(term))

	rf.state = state
	rf.term = term
	rf.voteFor = -1

	switch state {
	case Follower:
		//Leader -> Follower because of receiving from sever with higher term
		//Candidate -> Follower because of receiving from sever with higher term or receiving from leader
	case Candidate:
		//Follower -> Candidate because of election timer elapses
		//will vote for itself soon
	case Leader:
		//Candidate -> Leader because of winning election (vote from majority of servers)
		//rf.ResetHeartBeatTimer()

		//issue: send appendentries immediately or not?
		//ans: Yes?
		rf.HitHeartBeatTimer()
	default:
		common.PrintException("Server[" + strconv.Itoa(rf.me) + "]: Unknown state " + StateToString(state))
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Lock")
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
	//common.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Unlock")
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.term = 0
	rf.state = Follower
	rf.logEntries = make([]LogEntry, 0)
	rf.voteFor = -1

	rf.electionTimer = time.NewTimer(ElectionTimeout)
	rf.heartBeatTimer = time.NewTimer(HeartBeatTimeout)
	rf.ResetElectionTimer()
	rf.ResetHeartBeatTimer()

	rf.stopCh = make(chan struct{})

	rf.readPersist(persister.ReadRaftState())

	//Leader heartbeat append empty log to followers for failure detection
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.heartBeatTimer.C:
				rf.Lock()
				flag := rf.state == Leader
				rf.Unlock()
				if flag {
					rf.StartHeartBeatCheck()
				}
				rf.Lock()
				rf.ResetHeartBeatTimer()
				rf.Unlock()
			}
		}
	}()

	//Followers start election if election Timeout
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.Lock()
				flag := rf.state == Follower
				rf.Unlock()
				if flag {
					rf.Lock()
					//increase term when start election
					rf.ChangeState(Candidate, rf.term+1)
					rf.Unlock()
					rf.StartElection()
				} else {
					rf.Lock()
					rf.ResetElectionTimer()
					rf.Unlock()
				}
			}
		}
	}()

	//Apply log
	// go func(){

	// }()

	return rf
}
