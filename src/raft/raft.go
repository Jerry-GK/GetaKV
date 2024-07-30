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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../labutil"
)

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
	RPCSingleTimeout = time.Millisecond * 3000
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
	term              int
	voteFor           int // -1 if not voted yet
	logEntries        []LogEntry
	commitIndex       int //index of highest log entry known to be committed, initialized to 0, increase monotonically
	lastIncludedIndex int
	lastIncludedTerm  int
	// persistent states end

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	applyTimer     *time.Timer

	lastAppliedIndex int //index of highest log entry applied to state machine, initialized to 0, increase monotonically

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
	rf.lock()
	defer rf.unlock()
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
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

//must have outer lock!
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastAppliedIndex)
	data := w.Bytes()
	return data
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
	var commitIndex int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var lastAppliedIndex int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastAppliedIndex) != nil {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: readPersist failed while decoding!")
		labutil.PanicSystem()
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.logEntries = logEntries
		rf.commitIndex = commitIndex
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		//rf.lastAppliedIndex = lastAppliedIndex //issue: is lastAppliedIndex need to be persisted?
	}
}

//this function may be called directly by the server
func (rf *Raft) SavePersistAndSnapshot(index int, snapshot []byte) {
	rf.lock()
	defer rf.unlock()

	if index == 0 { //save before quit
		rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	}

	if index <= rf.lastIncludedIndex {
		return
	}

	if index > rf.commitIndex {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: SavePersistAndSnapshot failed, index > rf.commitIndex!")
		labutil.PanicSystem()
		return
	}

	// delete all log entries before lastIncludedIndex
	rf.setLogEntries(rf.getLogEntriesByIndexRange(index, 0))
	rf.setLastIncludedIndex(index)
	rf.setLastIncludedTerm(rf.getLogEntryByIndex(index).Term)
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
	rf.lock()
	defer rf.unlock()
	labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Receive Start Command")

	index := -1
	term := rf.term
	isLeader := false

	// Your code here (2B).
	isLeader = rf.state == Leader
	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.term
		//add new log entry
		newLogEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		//rf.logEntries = append(rf.logEntries, newLogEntry)
		rf.appendLogEntries(newLogEntry)
		rf.matchIndex[rf.me] = index

		rf.hitHeartBeatTimer()
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
	rf.persister.Close()
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() {
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: " + StateToString(rf.state) + " reset election timer")
	rf.electionTimer.Stop()
	// add random time to avoid all boom at the same time
	r := time.Duration(rand.Int63())%(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	rf.electionTimer.Reset(r)
}

func (rf *Raft) resetHeartBeatTimer() {
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: " + StateToString(rf.state) + " reset heartbeat timer")
	rf.heartBeatTimer.Stop()
	// add random time to avoid all boom at the same time
	//r := time.Duration(rand.Int63()) % HeartBeatTimeout
	//issue: need HeartBear timer need to be random?
	//ans: No
	rf.heartBeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) hitHeartBeatTimer() {
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: " + StateToString(rf.state) + " clear heartbeat timer")
	rf.heartBeatTimer.Stop()
	//almost elapsed immediately
	rf.heartBeatTimer.Reset(HitTinyInterval)
	//or rf.heartBeatTimer.Reset(0)? maybe dangerous!
}

func (rf *Raft) resetApplyTimer() {
	rf.applyTimer.Stop()
	rf.applyTimer.Reset(ApplyTimeout)
}

func (rf *Raft) hitApplyTimer() {
	rf.applyTimer.Stop()
	rf.applyTimer.Reset(HitTinyInterval)
	//or rf.applyTimer.Reset(0)? maybe dangerous!
}

//must have outer lock!
func (rf *Raft) setVoteFor(voteFor int) {
	rf.voteFor = voteFor
	if voteFor != InvalidVoteFor { //avoid redundant persist with changeState
		rf.persist()
	}
}

//must have outer lock!
func (rf *Raft) appendLogEntries(logEntries ...LogEntry) {
	rf.logEntries = append(rf.logEntries, logEntries...)
	rf.persist()
}

//must have outer lock!
func (rf *Raft) setLogEntries(logEntries []LogEntry) {
	rf.logEntries = logEntries
	rf.persist()
}

//must have outer lock!
func (rf *Raft) setLastIncludedIndex(index int) {
	rf.lastIncludedIndex = index
	rf.persist()
}

//must have outer lock!
func (rf *Raft) setLastIncludedTerm(term int) {
	rf.lastIncludedTerm = term
	rf.persist()
}

//must have outer lock!
func (rf *Raft) setCommitIndex(index int) {
	if index < rf.commitIndex {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: setCommitIndex: commitIndex cannot decrease!")
		labutil.PanicSystem()
	}

	// if index > rf.getLastLogIndex() {
	// 	labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: setCommitIndex: commitIndex cannot exceed the last log index!")
	// 	labutil.PanicSystem()
	// }

	rf.commitIndex = index

	if rf.commitIndex > rf.lastAppliedIndex {
		//trigger apply log
		rf.hitApplyTimer()
	}
	rf.persist()
}

func (rf *Raft) setLastApplied(lastAppliedIndex int) {
	rf.lastAppliedIndex = lastAppliedIndex
	//rf.persist()
}

//important function, the only way for server to change state or term
//change the state of the server, update the term
//must have outer lock!
func (rf *Raft) changeState(state State, term int) {
	// enable change to the same state / same term

	if state != rf.state {
		labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: changeState: " + StateToString(rf.state) + " -> " + StateToString(state) + ", term: " + fmt.Sprint(rf.term) + " -> " + fmt.Sprint(term))
	}

	rf.state = state

	if term < rf.term {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: Try to decrease term!")
		labutil.PanicSystem()
	}

	//only reset voteFor if term grows, to ensure at most one vote per term
	if term > rf.term {
		rf.setVoteFor(InvalidVoteFor)
		//persist for term change and voteFor reset
		rf.persist()
	}

	rf.term = term

	//reset election timer when changing state
	rf.resetElectionTimer()

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
		lastLogIndex := rf.getLastLogIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}

		//issue: send appendentries immediately or not?
		//ans: Yes?
		rf.hitHeartBeatTimer()
	default:
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: Unknown state " + StateToString(state))
	}
}

//must have outer lock!
func (rf *Raft) getLastLogIndex() int {
	//index starts from 1
	if rf.lastIncludedIndex == 0 {
		return len(rf.logEntries)
	} else {
		return len(rf.logEntries) + rf.lastIncludedIndex - 1
	}
}

//must have outer lock!
func (rf *Raft) getLastLogTerm() int {
	if len(rf.logEntries) == 0 {
		return -1
	}
	return rf.logEntries[len(rf.logEntries)-1].Term
}

//must have outer lock!
func (rf *Raft) getLogEntryByIndex(index int) LogEntry {
	physicalIndex := index
	if rf.lastIncludedIndex != 0 {
		physicalIndex = index - rf.lastIncludedIndex + 1
	}

	if physicalIndex-1 >= len(rf.logEntries) {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: len = " + fmt.Sprint(len(rf.logEntries)) + " physicalIndex = " + fmt.Sprint(physicalIndex) + " ,getLogEntryByIndex: physicalIndex out of range!")
		labutil.PanicSystem()
	}
	return rf.logEntries[physicalIndex-1]
}

//return log entries of index left -> right-1 (logEntries[left-1:right-1])
//getLogEntriesByIndexRange(left, 0) = logEntries[left-1:]
//must have outer lock!
func (rf *Raft) getLogEntriesByIndexRange(left int, right int) []LogEntry {
	physicalLeft := left
	if rf.lastIncludedIndex != 0 {
		physicalLeft = left - rf.lastIncludedIndex + 1
	}
	physicalRight := right
	if rf.lastIncludedIndex != 0 {
		physicalRight = right - rf.lastIncludedIndex + 1
	}

	if physicalLeft > physicalRight && right != 0 {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: GetLogEntriesByIndex: physicalLeft > physicalRight!")
		labutil.PanicSystem()
	}
	if physicalLeft < 1 {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: GetLogEntriesByIndex: physicalLeft < 1!")
		labutil.PanicSystem()
	}
	if right > rf.getLastLogIndex() {
		labutil.PrintException("Server[" + fmt.Sprint(rf.me) + "]: GetLogEntriesByIndex: right > lastLogIndex!")
		labutil.PanicSystem()
	}
	if right == 0 {
		physicalRight = rf.getLastLogIndex() + 1
		if rf.lastIncludedIndex != 0 {
			physicalRight = rf.getLastLogIndex() + 1 - rf.lastIncludedIndex + 1
		}
	}
	return rf.logEntries[physicalLeft-1 : physicalRight-1]
}

func (rf *Raft) lock() {
	rf.mu.Lock()
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Lock")
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
	//labutil.PrintDebug("Server[" + fmt.Sprint(rf.me) + "]: Unlock")
}

func (rf *Raft) startApplyLog() {
	rf.lock()

	var msgs []ApplyMsg
	if rf.lastAppliedIndex < rf.lastIncludedIndex { //this condition is critical
		msgs = make([]ApplyMsg, 0, 1)
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command:      nil,
			CommandIndex: rf.lastIncludedIndex,
		})
		//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: Invalid, lastAppliedIndex = " + fmt.Sprint(rf.lastAppliedIndex) + " lastIncludedIndex = " + fmt.Sprint(rf.lastIncludedIndex))
	} else if rf.commitIndex <= rf.lastAppliedIndex {
		//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: commitIndex <= lastAppliedIndex")
		msgs = make([]ApplyMsg, 0)
	} else {
		//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "]: ApplyLog")

		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastAppliedIndex)
		//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "] lastAppliedIndex = " + fmt.Sprint(rf.lastAppliedIndex))
		//labutil.PrintMessage("Server[" + fmt.Sprint(rf.me) + "] LastIncludedIndex = " + fmt.Sprint(rf.lastIncludedIndex))
		for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex && i <= rf.getLastLogIndex(); i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntryByIndex(i).Command,
				CommandIndex: i,
			})
		}
	}

	rf.unlock()

	//lock has to be released before sending to applyCh(whose size may be 1)
	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.lock()
		rf.setLastApplied(msg.CommandIndex) //lastAppliedIndex is updated here, even for invalid applyMsg
		//rf.commitIndex = labutil.MaxOfInt(rf.commitIndex, rf.lastAppliedIndex) //issue: is this necessary?
		rf.unlock()
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
	rf.resetElectionTimer()
	rf.resetHeartBeatTimer()
	rf.resetApplyTimer()

	rf.stopCh = make(chan struct{})
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastAppliedIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastIncludedIndex = 0          //issue: 0 or 1?, 0 means no snapshot yet
	rf.lastIncludedTerm = InvalidTerm //issue: 0 or -1?

	rf.readPersist(persister.ReadRaftState())

	rf.persist()

	//Leader heartbeat append empty log to followers for failure detection
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.heartBeatTimer.C:
				rf.lock()
				flag := rf.state == Leader
				rf.unlock()
				if flag {
					rf.startHeartBeat()
				}
				rf.lock()
				rf.resetHeartBeatTimer()
				rf.unlock()
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
				rf.lock()
				flag := rf.state == Follower
				rf.unlock()
				if flag {
					rf.lock()
					//increase term when start election
					rf.changeState(Candidate, rf.term+1)
					rf.unlock()
					rf.startElection()
				} else {
					rf.lock()
					rf.resetElectionTimer()
					rf.unlock()
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
				rf.startApplyLog()
				rf.lock()
				rf.resetApplyTimer()
				rf.unlock()
			}
		}
	}()

	return rf
}
