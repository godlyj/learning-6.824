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

import "sync"
import "../labrpc"
import "math/rand"
import "time"
import "fmt"
import "sort"
import "bytes"
import "encoding/gob"
import "sync/atomic"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	CommandValid       bool
	CommandIndex       int
	CommandTerm        int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

const HeartbeatInterval = 50
const VOTENULL = -1

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntries struct {
	Term     int "leader's term"
	LeaderId int "leader id"
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             RaftState
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	currentTerm int // all servers persistent
	votedFor    int // all servers persistent
	log         []LogEntry

	commitIndex int // all servers volatile
	lastApplied int // all servers volatile

	nextIndex  []int //only on leaders volatile
	matchIndex []int //only on leaders volatile

	applyCh       chan ApplyMsg
	appendEntryCh chan bool
	grantVoteCh   chan bool
	leaderCh      chan bool
	exitCh        chan bool
}

func (entry LogEntry) String() string {
	str := fmt.Sprintf("(i:%v,t:%v,c:%v)", entry.Index, entry.Term, entry.Command)
	return str
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//DPrintf("persist:%v, %v, %v", rf.currentTerm, rf.votedFor, rf.log)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int "candidate's term"
	CandidateId  int "candidate requestiong vote"
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  "currentTerm, for candidate to update itself"
	VoteGranted bool "true means candidate receive vote"
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	voteGranted := false
	if args.Term == rf.currentTerm && (rf.votedFor == VOTENULL || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		dropAndSet(rf.grantVoteCh)
		//DPrintf("%v vote %v my term:%d, vote term:%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
}

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	success := false
	conflictTerm := 0
	conflictIndex := 0

	// all servers
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		rf.state = Follower
		dropAndSet(rf.appendEntryCh)

		if args.PrevLogIndex > rf.getLastLogIndex() {
			conflictIndex = len(rf.log)
			conflictTerm = 0
		} else {
			prevLogTerm := rf.log[args.PrevLogIndex].Term
			if args.PrevLogTerm != prevLogTerm {
				conflictTerm = rf.log[args.PrevLogIndex].Term
				for i := 1; i < len(rf.log); i++ {
					if rf.log[i].Term == conflictTerm {
						conflictIndex = i
						break
					}
				}
			}

			if args.PrevLogIndex == 0 || (args.PrevLogIndex <= rf.getLastLogIndex() && args.PrevLogTerm == prevLogTerm) {
				success = true
				index := args.PrevLogIndex
				for i := 0; i < len(args.Entries); i++ {
					index += 1
					if index > rf.getLastLogIndex() {
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}

					if rf.log[index].Term != args.Entries[i].Term {
						//DPrintf("Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PrevLogIndex, index)
						for len(rf.log) > index {
							rf.log = rf.log[0 : len(rf.log)-1]
						}
						rf.log = append(rf.log, args.Entries[i])
					}
				}

				//DPrintf("Server(%v=>%v) term:%v, Handle AppendEntries Success", args.LeaderId, rf.me, rf.currentTerm)
				// Appendentries 5, 设置commitIndex为LeaderCommit和最后一个New Entry的较小值。
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = intMin(args.LeaderCommit, rf.getLastLogIndex())
				}
			}
		}
	}

	rf.applyLogs()

	reply.Term = rf.currentTerm
	reply.Success = success
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
	return
}
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	index := -1
	isleader := (rf.state == Leader)

	if isleader {
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}

		//注意append entry必须与index设置在一个加锁位置，如果推迟append，会导致concurrent start失败。
		rf.log = append(rf.log, entry)
		rf.persist()
	}

	return index, term, isleader
}

/**
If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm:
set commitIndex = N
*/
func (rf *Raft) advanceCommitIndex() {
	matchIndexes := make([]int, len(rf.matchIndex))
	copy(matchIndexes, rf.matchIndex)
	matchIndexes[rf.me] = len(rf.log) - 1
	sort.Ints(matchIndexes)

	N := matchIndexes[len(rf.peers)/2]
	//DPrintf("matchIndexes:%v, N:%v", matchIndexes, N)

	if rf.state == Leader && N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		//DPrintf("Server(%v) advanceCommitIndex (%v => %v)", rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.applyLogs()
	}
}

func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[idx]
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.log[nextIndex:]...)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ret := rf.sendAppendEntries(idx, args, reply)
				//DPrintf("SendAppendEntries (%v=>%v), args:%v", rf.me, idx, args)
				if !ret {
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if !rf.checkState(Leader, args.Term) {
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					// AppendEntries成功，更新对应raft实例的nextIndex和matchIndex值, Leader 5.3
					rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					//DPrintf("SendAppendEntries Success(%v => %v), nextIndex:%v, matchIndex:%v", rf.me, idx, rf.nextIndex, rf.matchIndex)
					rf.advanceCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					// AppendEntries失败，减小对应raft实例的nextIndex的值重试 Leader 5.3
					newIndex := reply.ConflictIndex
					for i := 1; i < len(rf.log); i++ {
						entry := rf.log[i]
						if entry.Term == reply.ConflictTerm {
							newIndex = i + 1
						}
					}
					rf.nextIndex[idx] = intMax(1, newIndex)
					//DPrintf("SendAppendEntries failed(%v => %v), decrease nextIndex(%v):%v", rf.me, idx, idx, rf.nextIndex)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// 对于所有服务器都需要执行的
func (rf *Raft) applyLogs() {
	//注意这里的for循环，如果写成if那就错了，会无法通过lab-2B的测试。
	for rf.commitIndex > rf.lastApplied {
		//DPrintf("Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied].Command)
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex:   entry.Index,
			Command: entry.Command,
			CommandTerm:   entry.Term,
		}
		rf.applyCh <- msg //applyCh在test_test.go中要用到
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//DPrintf("Kill Server(%v)", rf.me)
	dropAndSet(rf.exitCh)
}

func getRandomElectionTimeout() time.Duration {
	randomTimeout := 300 + rand.Intn(100)
	electionTimeout := time.Duration(randomTimeout) * time.Millisecond
	return electionTimeout
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	//DPrintf("Convert server(%v) state(%v=>candidate) term(%v)", rf.me,
		//rf.state.String(), rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var numVoted int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int, args RequestVoteArgs) {
			reply := &RequestVoteReply{}
			//DPrintf("sendRequestVote(%v=>%v) args:%v", rf.me, idx, args)
			ret := rf.sendRequestVote(idx, args, reply)
			if ret {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}

				if !rf.checkState(Candidate, args.Term) {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&numVoted, 1)
				}

				if atomic.LoadInt32(&numVoted) > int32(len(rf.peers)/2) {
					//DPrintf("Server(%d) win vote", rf.me)
					// 这两句调用顺序很重要
					rf.convertToLeader()
					dropAndSet(rf.leaderCh)
				}
			}
		}(i, args)
	}
}

func (rf *Raft) checkState(state RaftState, term int) bool {
	return rf.state == state && rf.currentTerm == term
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	//DPrintf("Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		//rf.state.String(), rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = VOTENULL
}

func (rf *Raft) getPrevLogIndex(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.getPrevLogIndex(idx)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.log[lastLogIndex].Term
	}
}

func (rf *Raft) convertToLeader() {
	defer rf.persist()
	if rf.state != Candidate {
		return
	}

	//DPrintf("Convert server(%v) state(%v=>leader) term %v", rf.me,
		//rf.state.String(), rf.currentTerm)
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
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
	rf.currentTerm = 0
	rf.votedFor = VOTENULL

	//如果slice的第一个元素为nil会导致gob Encode/Decode为空,这里改为一个空的LogEntry便于编码。
	rf.log = make([]LogEntry, 0)
	emptylog := LogEntry{}
	rf.log = append(rf.log, emptylog)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.applyCh = applyCh

	rf.exitCh = make(chan bool, 1)
	rf.grantVoteCh = make(chan bool, 1)
	rf.appendEntryCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)

	rf.heartbeatInterval = time.Duration(HeartbeatInterval) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//DPrintf("Make Server(%v)", rf.me)

	go func() {
	Loop:
		for {
			select {
			case <-rf.exitCh:
				//DPrintf("Exit Server(%v)", rf.me)
				break Loop
			default:
			}

			electionTimeout := getRandomElectionTimeout()
			rf.mu.Lock()
			state := rf.state
			//DPrintf("Server(%d) state:%v, electionTimeout:%v", rf.me, state, electionTimeout)
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Candidate:
				go rf.leaderElection()
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-rf.leaderCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(rf.heartbeatInterval)
			}
		}
	}()

	return rf
}