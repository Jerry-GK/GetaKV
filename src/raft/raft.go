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
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../labutil"
)

// import "bytes"
// import "../labgob"

// var RPC_AE_TotalCallNum = 0
// var RPC_RV_TotalCallNum = 0
// var RPC_ConcurrentCallNum = 0
// var HeartBeatNum = 0

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
	//1. ElectionTimeout is chose randomly in [ElectionTimeoutMin, ElectionTimeoutMax)
	//2. Min should be not smaller than 2 * HeartBeatTimeout
	//3. (Max - Min)/2 should be enough for collecting votes
	//4. Max should not be too big, otherwise it may cause too long time to elect a new leader
	ElectionTimeoutMin = time.Millisecond * 300 // election(both election-check interval and election timeout), min
	ElectionTimeoutMax = time.Millisecond * 600 // election(both election-check interval and election timeout), max

	HeartBeatTimeout = time.Millisecond * 150 // leader heartbeat

	ApplyTimeout = time.Millisecond * 100 // apply log

	// RPCSingleTimeout: may cause too long time to wait for a single RPC response if too big
	// RPCSingleTimeout: may canuse too many RPC Calls if too small
	RPCSingleTimeout = time.Millisecond * 100
	// RPCBatchTimeout: may ignore all RPC with long latency if too small
	// RPCBatchTimeout: may cause more RPC Calls and too long time to wait for a batch RPC response(in a RPC Caller) if too big,
	// which can be replaced by retry in the new RPC Caller of a new HeartBeat
	RPCBatchTimeout = time.Millisecond * 3000
	RPCInterval     = time.Millisecond * 20 // RPCInterval: may cause busy loop for RPC retry if too small

	HitTinyInterval = time.Millisecond * 5 // hit timer will reset it by this value

	InvalidTerm    = -1
	InvalidVoteFor = -1
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

	// state a Raft server must maintain.
	state State // 0: follower, 1: candidate, 2: leader

	// persistent states begin
	term       int
	voteFor    int // -1 if not voted yet
	logEntries []LogEntry
	// persistent states end

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	applyTimer     *time.Timer

	// for log replication
	commitIndex int //index of highest log entry known to be committed, initialized to 0, increase monotonically
	lastApplied int //index of highest log entry applied to state machine, initialized to 0, increase monotonically

	// Leader only. For each server, index of the next log entry to send to that server
	// Initialized to leader last log index + 1 after election success
	nextIndex []int
	// Leader only. For each server, index of highest log entry known to be replicated on server
	// Initialized to 0 after election success, increase monotonically
	matchIndex []int

	stopCh chan struct{}

	applyCh chan ApplyMsg
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
//must have outer lock!
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.GetPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
//must have outer lock!
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logEntries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logEntries) != nil {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: readPersist failed while decoding!")
		labutil.PanicSystem()
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.logEntries = logEntries
	}
}

func (rf *Raft) SavePersistAndSnapshot(state []byte, snapshot []byte) {
	rf.Lock()
	defer rf.Unlock()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
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
	rf.Lock()
	defer rf.Unlock()
	//println("Start Command")
	labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Receive Start Command")

	index := -1
	term := rf.term
	isLeader := false

	// Your code here (2B).
	isLeader = rf.state == Leader
	if isLeader {
		index = rf.GetLastLogIndex() + 1
		term = rf.term
		//add new log entry
		newLogEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		//rf.logEntries = append(rf.logEntries, newLogEntry)
		rf.AppendLogEntries(newLogEntry)
		rf.matchIndex[rf.me] = index

		rf.HitHeartBeatTimer()
	}
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
	//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " reset election timer")
	rf.electionTimer.Stop()
	// add random time to avoid all boom at the same time
	r := time.Duration(rand.Int63())%(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	rf.electionTimer.Reset(r)
}

func (rf *Raft) ResetHeartBeatTimer() {
	//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " reset heartbeat timer")
	rf.heartBeatTimer.Stop()
	// add random time to avoid all boom at the same time
	//r := time.Duration(rand.Int63()) % HeartBeatTimeout
	//issue: need HeartBear timer need to be random?
	//ans: No
	rf.heartBeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) HitHeartBeatTimer() {
	//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: " + StateToString(rf.state) + " clear heartbeat timer")
	rf.heartBeatTimer.Stop()
	//almost elapsed immediately
	rf.heartBeatTimer.Reset(HitTinyInterval)
	//or rf.heartBeatTimer.Reset(0)? maybe dangerous
}

func (rf *Raft) ResetApplyTimer() {
	rf.applyTimer.Stop()
	rf.applyTimer.Reset(ApplyTimeout)
}

func (rf *Raft) HitApplyTimer() {
	rf.applyTimer.Stop()
	rf.applyTimer.Reset(HitTinyInterval)
	//or rf.applyTimer.Reset(0)? maybe dangerous
}

//must have outer lock!
func (rf *Raft) SetVoteFor(voteFor int) {
	rf.voteFor = voteFor
	if voteFor != InvalidVoteFor { //avoid redundant persist with ChangeState
		rf.persist()
	}
}

//must have outer lock!
func (rf *Raft) AppendLogEntries(logEntries ...LogEntry) {
	rf.logEntries = append(rf.logEntries, logEntries...)
	rf.persist()
}

//must have outer lock!
func (rf *Raft) SetLogEntries(logEntries []LogEntry) {
	rf.logEntries = logEntries
	rf.persist()
}

//must have outer lock!
func (rf *Raft) SetCommitIndex(index int) {
	//println("Set Commit Index = " + strconv.Itoa(index))
	if index < rf.commitIndex {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: SetCommitIndex: commitIndex cannot decrease!")
		labutil.PanicSystem()
	}
	if index > rf.GetLastLogIndex() {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: SetCommitIndex: commitIndex cannot exceed the last log index!")
		labutil.PanicSystem()
	}

	labutil.PrintDebug("When set commitIndex, len(logEntries) = " + strconv.Itoa(len(rf.logEntries)))

	rf.commitIndex = index

	if rf.commitIndex > rf.lastApplied {
		//trigger apply log
		rf.HitApplyTimer()
	}
}

//important function, the only way for server to change state or term
//change the state of the server, update the term
//must have outer lock!
func (rf *Raft) ChangeState(state State, term int) {
	// enable change to the same state / same term

	if state != rf.state {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: ChangeState: " + StateToString(rf.state) + " -> " + StateToString(state) + ", term: " + strconv.Itoa(rf.term) + " -> " + strconv.Itoa(term))
	}

	rf.state = state

	if term < rf.term {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: Try to decrease term!")
		labutil.PanicSystem()
	}

	//only reset voteFor if term grows, to ensure at most one vote per term
	if term > rf.term {
		rf.SetVoteFor(InvalidVoteFor)
		//persist for term change and voteFor reset
		rf.persist()
	}

	rf.term = term

	//reset election timer when changing state
	rf.ResetElectionTimer()

	switch state {
	case Follower:
		//Leader -> Follower because of receiving from sever with higher term
		//Candidate -> Follower because of receiving from sever with higher term or receiving from leader
	case Candidate:
		//Follower -> Candidate because of election timer elapses
		//will vote for itself soon
	case Leader:
		//Candidate -> Leader because of winning election (vote from majority of servers)
		//initialize nextIndex and matchIndex for each server after election success
		lastLogIndex := rf.GetLastLogIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}

		//issue: send appendentries immediately or not?
		//ans: Yes?
		rf.HitHeartBeatTimer()
	default:
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: Unknown state " + StateToString(state))
	}
}

//must have outer lock!
func (rf *Raft) GetLastLogIndex() int {
	//index starts from 1
	return len(rf.logEntries)
}

//must have outer lock!
func (rf *Raft) GetLastLogTerm() int {
	if len(rf.logEntries) == 0 {
		return -1
	}
	return rf.logEntries[len(rf.logEntries)-1].Term
}

//must have outer lock!
func (rf *Raft) GetLogEntryByIndex(index int) LogEntry {
	//idx := index - rf.lastsnapshotindex
	if index-1 >= len(rf.logEntries) {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: len = " + strconv.Itoa(len(rf.logEntries)) + " index = " + strconv.Itoa(index) + " ,GetLogEntryByIndex: index out of range!")
		labutil.PanicSystem()
	}
	return rf.logEntries[index-1]
}

//return log entries of index left -> right-1 (lofEntries[left-1:right-1])
//GetLogEntriesByIndexRange(left, 0) = logEntries[left-1:]
//must have outer lock!
func (rf *Raft) GetLogEntriesByIndexRange(left int, right int) []LogEntry {
	if left > right && right != 0 {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: GetLogEntriesByIndex: left > right!")
		labutil.PanicSystem()
	}
	if left < 1 {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: GetLogEntriesByIndex: left < 1!")
		labutil.PanicSystem()
	}
	if right > rf.GetLastLogIndex() {
		labutil.PrintException("Server[" + strconv.Itoa(rf.me) + "]: GetLogEntriesByIndex: right > lastLogIndex!")
		labutil.PanicSystem()
	}
	if right == 0 {
		right = rf.GetLastLogIndex() + 1
	}
	return rf.logEntries[left-1 : right-1]
}

//must have outer lock!
func (rf *Raft) GetPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	return data
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
	//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Lock")
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
	//labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Unlock")
}

func (rf *Raft) StartApplyLog() {
	rf.Lock()

	var msgs []ApplyMsg
	msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msgs = append(msgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.GetLogEntryByIndex(i).Command,
			CommandIndex: i,
		})
	}

	if len(msgs) != 0 {
		labutil.PrintDebug("Server[" + strconv.Itoa(rf.me) + "]: Start apply non-empry log entries")
	}

	rf.Unlock()

	//lock has to be released before sending to applyCh(whose size may be 1)
	for _, msg := range msgs {
		//println("Try to apply msg")
		rf.applyCh <- msg
		rf.Lock()
		rf.lastApplied = msg.CommandIndex
		rf.Unlock()
		//println("Applied msg")
	}
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
	rf.state = Follower

	rf.term = 0
	rf.voteFor = InvalidVoteFor
	rf.logEntries = make([]LogEntry, 0)

	rf.electionTimer = time.NewTimer(ElectionTimeoutMax)
	rf.heartBeatTimer = time.NewTimer(HeartBeatTimeout)
	rf.applyTimer = time.NewTimer(ApplyTimeout)
	rf.ResetElectionTimer()
	rf.ResetHeartBeatTimer()
	rf.ResetApplyTimer()

	rf.stopCh = make(chan struct{})
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.readPersist(persister.ReadRaftState())

	rf.persist()

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
					rf.StartHeartBeat()
					//HeartBeatNum++
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
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.StartApplyLog()
				rf.Lock()
				rf.ResetApplyTimer()
				rf.Unlock()
			}
		}
	}()

	// //Show Counting Variables
	// go func() {
	// 	for {
	// 		//labutil.PrintMessage("HeartBeatNum = " + strconv.Itoa(HeartBeatNum))
	// 		//labutil.PrintMessage("RPC_AE_TotalCallNum = " + strconv.Itoa(RPC_AE_TotalCallNum))
	// 		//labutil.PrintMessage("RPC_RV_TotalCallNum = " + strconv.Itoa(RPC_RV_TotalCallNum))
	// 		//labutil.PrintMessage("RPC_ConcurrentCallNum = " + strconv.Itoa(RPC_ConcurrentCallNum))
	// 		//labutil.PrintMessage("-----------------------------")
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }()

	return rf
}
