package raft
//
////
//// this is an outline of the API that raft must expose to
//// the service (or tester). see comments below for
//// each of these functions for more details.
////
//// rf = Make(...)
////   create a new Raft server.
//// rf.Start(command interface{}) (index, term, isleader)
////   start agreement on a new log entry
//// rf.GetState() (term, isLeader)
////   ask a Raft for its current term, and whether it thinks it is leader
//// ApplyMsg
////   each time a new entry is committed to the log, each Raft peer
////   should send an ApplyMsg to the service (or tester)
////   in the same server.
////
//
//import (
//	"bytes"
//	//"fmt"
//	"math/rand"
//	"sort"
//	"sync"
//	"time"
//)
//import "sync/atomic"
//import "../labrpc"
//
//// import "bytes"
//import "../labgob"
//
////
//// as each Raft peer becomes aware that successive log entries are
//// committed, the peer should send an ApplyMsg to the service (or
//// tester) on the same server, via the applyCh passed to Make(). set
//// CommandValid to true to indicate that the ApplyMsg contains a newly
//// committed log entry.
////
//// in Lab 3 you'll want to send other kinds of messages (e.g.,
//// snapshots) on the applyCh; at that point you can add fields to
//// ApplyMsg, but set CommandValid to false for these other uses.
////
//type ApplyMsg struct {
//	CommandValid bool
//	Command      interface{}
//	CommandIndex int
//	CommandTerm int
//}
//type LogEntry struct {
//	Term    int
//	Index   int
//	Command interface{}
//}
//const(
//	Leader = 0
//	Follower = 1
//	Candidate = 2
//	Dead = 3
//)
//
//
////
//// A Go object implementing a single Raft peer.
////
//type Raft struct {
//	mu        sync.Mutex          // Lock to protect shared access to this peer's state
//	pmu       sync.Mutex
//	peers     []*labrpc.ClientEnd // RPC end points of all peers
//
//	persister *Persister          // Object to hold this peer's persisted state
//	me        int                 // this peer's index into peers[]
//	dead      int32               // set by Kill()
//
//	votedFor int
//	log       []LogEntry
//	CurrentTerm int
//	LastHeartBeatTime time.Time
//	state     int // leader or follower or candidate
//	applyCh chan ApplyMsg
//
//	commitIndex int
//	lastApplied int
//
//	//for leader
//	nextIndex  []int
//	matchIndex []int
//	// Your data here (2A, 2B, 2C).
//	// Look at the paper's Figure 2 for a description of what
//	// state a Raft server must maintain.
//
//}
//
//// return currentTerm and whether this server
//// believes it is the leader.
//func (rf *Raft) GetState() (int, bool) {
//
//	var term int
//	var isleader bool
//	// Your code here (2A).
//	rf.mu.Lock()
//	term = rf.CurrentTerm
//	if rf.state == Leader{
//		isleader = true
//	} else {
//		isleader = false
//	}
//	rf.mu.Unlock()
//	return term, isleader
//}
//
////
//// save Raft's persistent state to stable storage,
//// where it can later be retrieved after a crash and restart.
//// see paper's Figure 2 for a description of what should be persistent.
////
//func (rf *Raft) persist() {
//	// Your code here (2C).
//	// Example:
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(rf.CurrentTerm)
//	e.Encode(rf.votedFor)
//	e.Encode(rf.log)
//	data := w.Bytes()
//	rf.persister.SaveRaftState(data)
//}
//
////
//// restore previously persisted state.
////func randTimeout(lower int,upper int) int {
////	r := rand.New(rand.NewSource(time.Now().UnixNano()))
////	diff := upper - lowervariable/field VoteGranted
////	return lower + r.Intn(diff)
////}
//func (rf *Raft) readPersist(data []byte) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	if data == nil || len(data) < 1 { // bootstrap without any state?
//		return
//	}
//
//	r := bytes.NewBuffer(data)
//	d := labgob.NewDecoder(r)
//	d.Decode(&rf.CurrentTerm)
//	d.Decode(&rf.votedFor)
//	d.Decode(&rf.log)
//
//	// Your code here (2C).
//	// Example:
//	// r := bytes.NewBuffer(data)
//	// d := labgob.NewDecoder(r)
//	// var xxx
//	// var yyy
//	// if d.Decode(&xxx) != nil ||
//	//    d.Decode(&yyy) != nil {
//	//   error...
//	// } else {
//	//   rf.xxx = xxx
//	//   rf.yyy = yyy
//	// }
//}
//
//
//func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply )bool{
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	return ok
//}
//
//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	// Your code here (2A, 2B).
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//
//	isSuccess := false
//	conflictTerm := 0
//	conflictIndex := 0
//	rf.LastHeartBeatTime = time.Now()
//
//	//if args.Prevlogindex>=0 && args.Prevlogindex<len(rf.log){
//	//	prevLogTerm = rf.log[args.Prevlogindex].Term
//	//}
//
//	if args.Term > rf.CurrentTerm{
//		rf.pmu.Lock()
//		//DPrintf("get heartbeat becomeFollower: %d  term:%d\n",rf.me,rf.CurrentTerm)
//		rf.pmu.Unlock()
//		rf.becomeFollower(args.Term)
//	}
//	//false
//	if args.Term < rf.CurrentTerm {
//		goto  process
//	}
//	//false
//	if len(rf.log)-1 < args.Prevlogindex {
//		conflictIndex = len(rf.log)
//		goto process
//	}
//	//false
//	if args.Prevlogindex > 0 && rf.log[args.Prevlogindex].Term != args.Prevlogterm {
//		conflictTerm = rf.log[args.Prevlogindex].Term
//		for i := 0; i < len(rf.log); i++ {
//			if rf.log[i].Term == conflictTerm {
//				conflictIndex = i
//				break
//			}
//		}
//		goto process
//	}
//
//	//
//	//if args.Prevlogindex>=0 && args.Prevlogterm != prevLogTerm {
//	//	conflictTerm = rf.log[args.Prevlogindex].Term
//	//	for i := 0; i < len(rf.log); i++ {
//	//		if rf.log[i].Term == conflictTerm {
//	//			conflictIndex = i
//	//			break
//	//		}
//	//	}
//	//}
//
//	//save log
//	for i := 0; i < len(args.Entries); i++{
//		index := args.Prevlogindex+i+1
//		if index>len(rf.log) -1{
//			rf.log = append(rf.log,args.Entries[i])
//		}else{
//			if rf.log[index].Term != args.Entries[i].Term{
//				rf.log = rf.log[:index]
//				rf.log = append(rf.log,args.Entries[i])
//			}
//		}
//
//	}
//	rf.persist()
//
//	if args.Leadercommit > rf.commitIndex {
//		rf.commitIndex = args.Leadercommit
//		if len(rf.log)-1 < rf.commitIndex {
//			rf.commitIndex = len(rf.log) - 1
//		}
//	}
//
//	isSuccess = true
//	goto process
//
//process:
//	rf.applyLog()
//	reply.Success = isSuccess
//	reply.Term = rf.CurrentTerm
//	reply.ConflictIndex = conflictIndex
//	reply.ConflictTerm = conflictTerm
//	return
//}
//
//
//type AppendEntriesArgs struct {
//	Term int
//	Leaderid int
//	Prevlogindex int
//	Prevlogterm int
//	Entries []LogEntry
//	Leadercommit int
//}
//
////
//// example RequestVote RPC reply structure.
//// field names must start with capital letters!
////
//type AppendEntriesReply struct {
//	Term int
//	Success bool
//
//	//speed up
//	ConflictIndex int
//	ConflictTerm int
//
//	// Your data here (2A).
//}
//
////
//// example RequestVote RPC arguments structure.
//// field names must start with capital letters!
////
//type RequestVoteArgs struct {
//	// Your data here (2A, 2B).
//	Term int
//	CandidateId int
//	LastLogIndex int
//	LastLogTerm int
//
//}
//
////
//// example RequestVote RPC reply structure.
//// field names must start with capital letters!
////
//type RequestVoteReply struct {
//	// Your data here (2A).
//	Term int
//	VoteGranted bool
//}
//
////
//// example RequestVote RPC handler.
////
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	rf.LastHeartBeatTime = time.Now()
//	//fmt.Printf("CurrentTerm:%d args:%d\n",rf.CurrentTerm,args.Term)apply
//
//
//
//	if args.Term < rf.CurrentTerm{
//		reply.VoteGranted = false
//		reply.Term = rf.CurrentTerm
//		return
//	}
//	//fmt.Println("candidate: ",args.CandidateId,args.Term,"me: ",rf.me,rf.CurrentTerm,rf.votedFor)
//	//??
//	if args.Term >rf.CurrentTerm{
//		rf.pmu.Lock()
//		//DPrintf(" %d get request , has higher candidate election %d term %d becomeFollower:  term:%d state:%d\n",rf.me,args.CandidateId,args.Term,rf.CurrentTerm,rf.state)
//		rf.pmu.Unlock()
//		rf.becomeFollower(args.Term)
//	}
//	rf.pmu.Lock()
//	//DPrintf(" args.prelogindex %d args.prelogterm %d rf.lastlogindex %d rf.lastlogterm %d \n",args.LastLogIndex,args.LastLogTerm,rf.getLastLogIndex(),rf.getLastLogTerm())
//	rf.pmu.Unlock()
//	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogMoreUpToDate(args.LastLogIndex,args.LastLogTerm){
//		rf.votedFor = args.CandidateId
//		reply.VoteGranted = true
//		rf.persist()
//	}
//
//	//DPrintf("node:%d Voted for:%d\n",rf.me,rf.votedFor)
//
//	// Your code here (2A, 2B).
//}
//func (rf *Raft) isLogMoreUpToDate(index int, term int) bool {
//	return term > rf.getLastLogTerm() || (term == rf.getLastLogTerm() && index >= rf.getLastLogIndex())
//}
//
////
//// example code to send a RequestVote RPC to a server.
//// server is the index of the target server in rf.peers[].
//// expects RPC arguments in args.
//// fills in *reply with RPC reply, so caller should
//// pass &reply.
//// the types of the args and reply passed to Call() must be
//// the same as the types of the arguments declared in the
//// handler function (including whether they are pointers).
////
//// The labrpc package simulates a lossy network, in which servers
//// may be unreachable, and in which requests and replies may be lost.
//// Call() sends a request and waits for a reply. If a reply arrives
//// within a timeout interval, Call() returns true; otherwise
//// Call() returns false. Thus Call() may not return for a while.
//// A false return can be caused by a dead server, a live server that
//// can't be reached, a lost request, or a lost reply.
////
//// Call() is guaranteed to return (perhaps after a delay) *except* if the
//// handler function on the server side does not return.  Thus there
//// is no need to implement your own timeouts around Call().
////
//// look at the comments in ../labrpc/labrpc.go for more details.
////
//// if you're having trouble getting RPC to work, check that you've
//// capitalized all field names in structs passed over RPC, and
//// that the caller passes the address of the reply struct with &, not
//// the struct itself.
////
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	//fmt.Printf("SEND CurrentTerm:%d args:%d\n",rf.CurrentTerm,args.Term)
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}
//
////
//// the service using Raft (e.g. a k/v server) wants to start
//// agreement on the next command to be appended to Raft's log. if this
//// server isn't the leader, returns false. otherwise start the
//// agreement and return immediately. there is no guarantee that this
//// command will ever be committed to the Raft log, since the leader
//// may fail or lose an election. even if the Raft instance has been killed,
//// this function should return gracefully.
////
//// the first return value is the index that the command will appear at
//// if it's ever committed. the second return value is the current
//// term. the third return value is true if this server believes it is
//// the leader.
////
//func (rf *Raft) Start(command interface{}) (int, int, bool) {
//	//rf.mu.Lock()
//	//defer rf.mu.Unlock()
//
//	index := -1
//	term := -1
//	isLeader := true
//
//	// Your code here (2B).
//	term,isLeader = rf.GetState()
//
//
//	//new log
//
//	if rf.state!=Leader{
//		return -1,-1,false
//	}
//
//	index = rf.getLastLogIndex() +2
//	entry := LogEntry{
//		Term: term,
//		Index: index,
//		Command: command,
//	}
//	rf.log = append(rf.log,entry)
//	rf.persist()
//
//	rf.pmu.Lock()
//	//DPrintf("leader %d new log ,now %d",rf.me,len(rf.log))
//	rf.pmu.Unlock()
//	return index, term, isLeader
//}
//
////
//// the tester doesn't halt goroutines created by Raft after each test,
//// but it does call the Kill() method. your code can use killed() to
//// check whether Kill() has been called. the use of atomic avoids the
//// need for a lock.
////./
//// the issue is that long-running goroutines use memory and may chew
//// up CPU time, perhaps causing later tests to fail and generating
//// confusing debug output. any goroutine with a long-running loop
//// should call killed() to check whether it should stop.
////
//func (rf *Raft) Kill() {
//	atomic.StoreInt32(&rf.dead, 1)
//	// Your code here, if desired.
//}
//
//func (rf *Raft) killed() bool {
//	z := atomic.LoadInt32(&rf.dead)
//	return z == 1
//}
//
////
//// the service or tester wants to create a Raft server. the ports
//// of all the Raft servers (including this one) are in peers[]. this
//// server's port is peers[me]. all the servers' peers[] arrays
//// have the same order. persister is a place for this server to
//// save its persistent state, and also initially holds the most
//// recent saved state, if any. applyCh is a channel on which the
//// tester or service expects Raft to send ApplyMsg messages.
//// Make() must return quickly, so it should start goroutines
//// for any long-running work.
////
//func Make(peers []*labrpc.ClientEnd, me int,
//	persister *Persister, applyCh chan ApplyMsg) *Raft {
//	rf := &Raft{}
//	rf.peers = peers
//	rf.persister = persister
//	rf.me = me
//	rf.applyCh = applyCh
//	//fmt.Println("make raft ",me,rf.CurrentTerm)
//	//DPrintf("make raft node:%d term:%d\n",me,rf.CurrentTerm)
//	rf.votedFor = -1
//	rf.log = make([]LogEntry, 0)
//	rf.commitIndex = -1
//	rf.lastApplied = -1
//	rf.state = Follower
//	rf.CurrentTerm = 0
//	rf.LastHeartBeatTime = time.Now()
//	go rf.electionTimer()
//	//go rf.applyLogLoop()
//	//go rf.print()
//	// Your initialization code here (2A, 2B, 2C).
//
//	// initialize from state persisted before a crash
//	rf.readPersist(persister.ReadRaftState())
//
//	return rf
//}
//
//func (rf *Raft) print(){
//
//	time.Sleep(time.Duration(50) * time.Millisecond)
//	//DPrintf("nodeid:%d state:%d term:%d\n",rf.me,rf.state,rf.CurrentTerm)
//	//fmt.Println("nodeid: ",rf.me,"  state",rf.state,"  term",rf.CurrentTerm)
//}
//
//func (rf *Raft) heartBeatTimer() {
//	for {
//		rf.mu.Lock()
//		if rf.state != Leader || rf.killed() {
//			rf.mu.Unlock()
//			return
//		}
//
//		rf.mu.Unlock()
//		go rf.SendHeartbeat()
//
//		time.Sleep(time.Duration(100) * time.Millisecond)
//	}
//}
//
//func (rf *Raft) getPrevLogIndex(idx int) int {
//	return rf.nextIndex[idx] - 1
//}
//
//func (rf *Raft) getPrevLogTerm(idx int) int {
//	prevLogIndex := rf.getPrevLogIndex(idx)
//	if prevLogIndex <= 0 {
//		return -1
//	} else {
//		return rf.log[prevLogIndex].Term
//	}
//}
//func (rf *Raft) SendHeartbeat() {
//
//	for i := 0; i < len(rf.peers); i++ {
//		if i != rf.me {
//			rf.pmu.Lock()
//			//DPrintf("%d heartbeat to %d\n",rf.me,i)
//			rf.pmu.Unlock()
//			go rf.SendHeartbeatEntries(i)
//			time.Sleep(time.Duration(10) * time.Millisecond)
//		}
//	}
//}
//
//func (rf *Raft) SendHeartbeatEntries(p int) {
//
//		rf.mu.Lock()
//		if rf.state != Leader {
//			rf.mu.Unlock()
//			return
//		}
//		nextIndex := rf.nextIndex[p]
//		entries := make([]LogEntry, 0)
//		entries = append(entries, rf.log[nextIndex:]...)
//		args := AppendEntriesArgs{
//			Term:         rf.CurrentTerm,
//			Leaderid:     rf.me,
//			Entries:      entries,
//			Prevlogindex: rf.getPrevLogIndex(p),
//			Prevlogterm:  rf.getPrevLogTerm(p),
//			Leadercommit: rf.commitIndex,
//		}
//		reply := AppendEntriesReply{}
//		rf.mu.Unlock()
//		rf.pmu.Lock()
//		//DPrintf("leader %d  rf.me:%d rf.state %d send entry: nextindex:%d prelogindex:%d prelogterm:%d leadercommit:%d\n",args.Leaderid,rf.me,rf.state,nextIndex,args.Prevlogindex,args.Prevlogterm,args.Leadercommit)
//		//DPrintf("leader %d  rf.me:%d send entry: nextindex:%d prelogindex:%d prelogterm:%d leadercommit:%d entrylen:%d\n",args.Leaderid,rf.me,nextIndex,args.Prevlogindex,args.Prevlogterm,args.Leadercommit,len(args.Entries))
//		rf.pmu.Unlock()
//		ok := rf.sendAppendEntries(p, &args, &reply)
//		if !ok {
//			rf.pmu.Lock()
//			//DPrintf("fail %d\n",p)
//			rf.pmu.Unlock()
//			return
//		}
//		//
//		//rf.pmu.Lock()
//		//DPrintf("leader %d get, reply.term %d curterm %d\n",rf.me,)
//		//rf.pmu.Unlock()
//
//
//		rf.mu.Lock()
//		defer rf.mu.Unlock()
//		if rf.CurrentTerm != args.Term {
//			rf.pmu.Lock()
//			//DPrintf("cur term:%d ;  args term %d\n",rf.CurrentTerm,args.Term)
//			rf.pmu.Unlock()
//			return
//		}
//		if reply.Term > rf.CurrentTerm {
//			rf.pmu.Lock()
//			//DPrintf("heartbeat leader back to Follower: %d  term:%d\n",rf.me,rf.CurrentTerm)
//			rf.pmu.Unlock()
//			rf.becomeFollower(reply.Term)
//		}
//
//		if reply.Success {
//			rf.pmu.Lock()
//			//DPrintf("leader %d follower %d :reply success: prelogindex:%d  len:%d\n",rf.me,p,args.Prevlogindex,len(args.Entries))
//			rf.pmu.Unlock()
//			rf.matchIndex[p] = args.Prevlogindex + len(args.Entries)
//			rf.nextIndex[p] = rf.matchIndex[p]+1
//			//commit
//			rf.advanceCommitIndex()
//			return
//		}else{
//			rf.pmu.Lock()
//			//DPrintf("leader %d follower %d :reply fail prelogindex:%d  len:%d\n",rf.me,p,args.Prevlogindex,len(args.Entries))
//			rf.pmu.Unlock()
//			//rf.nextIndex[p] = args.Prevlogindex
//			//newIndex := reply.ConflictIndex
//			//for i := 0; i < len(rf.log); i++ {
//			//	entry := rf.log[i]
//			//	if entry.Term == reply.ConflictTerm {
//			//		newIndex = i + 1
//			//	}
//			//}
//			//rf.nextIndex[p] = intMax(1, newIndex)
//
//			lastEntryOfConflictTerm := -1
//			for entryIndex := args.Prevlogindex; entryIndex > 0; entryIndex-- {
//				if rf.log[entryIndex].Term == reply.ConflictTerm {
//					lastEntryOfConflictTerm = entryIndex
//					break
//				}
//			}
//			//If it does not find an entry with that term, it should set nextIndex = conflictIndex
//			if lastEntryOfConflictTerm == -1 {
//				rf.nextIndex[p] = reply.ConflictIndex
//			} else {
//				//If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
//				rf.nextIndex[p] = lastEntryOfConflictTerm + 1
//			}
//
//		}
//}
//
//func intMax(a int , b int) int {
//	if a>b{
//		return a
//	} else{
//		return b
//	}
//}
//
////func (rf *Raft) SendHeartbeat() {
////	for !rf.killed() {
////		time.Sleep(10 * time.Millisecond)
////		func() {
////			rf.mu.Lock()
////			defer rf.mu.Unlock()
////
////			if rf.state != Leader {
////				return
////			}
////			time.Sleep(time.Duration(100) * time.Millisecond)
////
////			for i := 0; i < len(rf.peers); i++ {
////				if i == rf.me {
////					continue
////				}
////
////				nextIndex := rf.nextIndex[i]
////				entries := make([]LogEntry, 0)
////				entries = append(entries, rf.log[nextIndex:]...)
////				args := AppendEntriesArgs{
////					Term:         rf.CurrentTerm,
////					Leaderid:     rf.me,
////					Entries:      entries,
////					Prevlogindex: rf.getPrevLogIndex(i),
////					Prevlogterm:  rf.getPrevLogTerm(i),
////					Leadercommit: rf.commitIndex,
////				}
////
////				go func(p int, args *AppendEntriesArgs) {
////					reply := AppendEntriesReply{}
////					ok := rf.sendAppendEntries(p, args, &reply)
////					if !ok {
////						return
////					}
////					rf.mu.Lock()
////					defer rf.mu.Unlock()
////					if rf.CurrentTerm != args.Term {
////						return
////					}
////					if reply.Term > rf.CurrentTerm {
////						rf.becomeFollower(reply.Term)
////						return
////					}
////					if reply.Success == true {
////						//如果成功：更新相应跟随者的 nextIndex 和 matchIndex\
////									rf.pmu.Lock()
////									DPrintf("heartbeat leader back to Follower: %d  term:%d\n",rf.me,rf.CurrentTerm)
////									rf.pmu.Unlock()
////						rf.matchIndex[p] = args.Prevlogindex + len(args.Entries)
////						rf.nextIndex[p] = rf.matchIndex[p] + 1
////						rf.advanceCommitIndex()
////					} else {
////						rf.nextIndex[p] = args.Prevlogindex
////					}
////				}(i, &args)
////			}
////		}()
////	}
////}
//
//
//func (rf *Raft) advanceCommitIndex() {
//	sortedMatchIndex := make([]int, len(rf.matchIndex))
//	copy(sortedMatchIndex, rf.matchIndex)
//	sortedMatchIndex[rf.me] = len(rf.log) - 1
//	rf.pmu.Lock()
//	//DPrintf("node %d matchindex %d   lastapplied :%d:  ",rf.me,len(sortedMatchIndex),rf.lastApplied)
//	rf.pmu.Unlock()
//	sort.Ints(sortedMatchIndex)
//	N := sortedMatchIndex[len(rf.peers)/2]
//
//	rf.pmu.Lock()
//	//DPrintf("N %d rf.commitIndex %d ",N,rf.commitIndex)
//	for i:=0;i<len(rf.matchIndex);i++{
//		//DPrintf("%d",rf.matchIndex[i])
//	}
//	rf.pmu.Unlock()
//	if rf.state == Leader && N > rf.commitIndex && len(rf.log)>0 && rf.log[N].Term == rf.CurrentTerm  {
//		rf.commitIndex = N
//
//		rf.applyLog()
//	}
//}
//func (rf *Raft) applyLog() {
//
//	commit := false
//
//
//	for rf.commitIndex > rf.lastApplied && len(rf.log)>0{
//		commit = true
//		rf.lastApplied += 1
//		entry := rf.log[rf.lastApplied]
//		msg := ApplyMsg{
//			CommandValid: true,
//			Command:      entry.Command,
//			CommandIndex: entry.Index,
//			CommandTerm:  entry.Term,
//		}
//		rf.applyCh <- msg
//	}
//	if commit{
//		if rf.state == Leader {
//			rf.pmu.Lock()
//			//DPrintf(" leader %d commit %d",rf.me,rf.commitIndex)
//			rf.pmu.Unlock()
//		}else{
//			rf.pmu.Lock()
//			//DPrintf(" follower %d commit %d",rf.me,rf.commitIndex)
//			rf.pmu.Unlock()
//		}
//	}
//
//}
//
//func (rf* Raft) electionTimer() {
//	for rf.state != Leader {
//		electionTimeout := 400 + rand.Intn(100)
//		startTime := time.Now()
//		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
//		rf.mu.Lock()
//		if rf.state == Dead {
//			rf.mu.Unlock()
//			return
//		}
//		if rf.LastHeartBeatTime.Before(startTime) {
//			if rf.state != Leader {
//				rf.pmu.Lock()
//				//DPrintf("node %d state %d start election curterm %d peers %d\n",rf.me,rf.state,rf.CurrentTerm,len(rf.peers))
//				rf.pmu.Unlock()
//				go rf.Handleelection()
//			}
//		}
//		rf.mu.Unlock()
//	}
//	//DPrintf("%v end election timer \n",rf.me)
//}
//
//
//func (rf *Raft) Handleelection() {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	granted := 1
//
//	request := RequestVoteArgs{}
//	rf.becomeCandidate()
//	rf.LastHeartBeatTime = time.Now()
//	request.Term = rf.CurrentTerm
//	request.CandidateId = rf.me
//	request.LastLogIndex = rf.getLastLogIndex()
//	request.LastLogTerm = rf.getLastLogTerm()
//
//	for i:=0;i<len(rf.peers);i++{
//		if i==rf.me{
//			continue
//		}
//		go func(p int){
//			reply := RequestVoteReply{}
//			ok := rf.sendRequestVote(p,&request,&reply)
//			if ok {
//				rf.mu.Lock()
//				defer rf.mu.Unlock()
//				if rf.state!=Candidate || rf.CurrentTerm!=request.Term{
//					return
//				}
//				if !reply.VoteGranted{
//					if reply.Term>rf.CurrentTerm{
//						rf.votedFor = -1
//						rf.pmu.Lock()
//						//DPrintf("Candidate back to Follower: %d  term:%d\n",rf.me,rf.CurrentTerm)
//						rf.pmu.Unlock()
//						rf.becomeFollower(reply.Term)
//					}
//					return
//				}
//				granted++
//				if granted > len(rf.peers) / 2 {
//					rf.becomeLeader()
//					go rf.heartBeatTimer()
//					return
//				}
//
//			}
//		}(i)
//		//fmt.Println(rf.me,granted)
//	}
//	//Your code here, if desired.
//}
//func (rf *Raft) getLastLogIndex() int {
//	return len(rf.log) - 1
//}
//
//func (rf *Raft) getLastLogTerm() int {
//	lastLogIndex := rf.getLastLogIndex()
//	if lastLogIndex <= 0 {
//		return 0
//	} else {
//		return rf.log[lastLogIndex].Term
//	}
//}
//func (rf* Raft) becomeLeader() {
//	defer rf.persist()
//	//
//	//fmt.Printf("becomeleader %d\n",rf.me)
//	rf.pmu.Lock()
//	//DPrintf("becomeleader: %d  term:%d\n",rf.me,rf.CurrentTerm)
//	rf.pmu.Unlock()
//
//	rf.state = Leader
//	//rf.CurrentTerm++
//	rf.nextIndex = make([]int, len(rf.peers))
//	for i := 0; i < len(rf.peers); i++ {
//		rf.nextIndex[i] = rf.getLastLogIndex() + 1
//	}
//	//don't know where match
//	rf.matchIndex = make([]int, len(rf.peers))
//
//}
//
//func (rf* Raft) becomeCandidate() {
//	//fmt.Printf("becomeCandidate %d\n",rf.me)
//	defer rf.persist()
//	rf.votedFor = rf.me
//	rf.state = Candidate
//	rf.CurrentTerm++
//	rf.pmu.Lock()
//	//DPrintf("becomeCandidate: %d  term:%d\n",rf.me,rf.CurrentTerm)
//	rf.pmu.Unlock()
//
//
//}
//
//func (rf* Raft) becomeFollower(term int) {
//
//	defer rf.persist()
//	rf.state = Follower
//	if term > rf.CurrentTerm {
//		rf.CurrentTerm = term
//	}
//	rf.votedFor = -1
//
//	rf.pmu.Lock()
//	//DPrintf("becomefollower: %d  term:%d\n",rf.me,rf.CurrentTerm)
//	rf.pmu.Unlock()
//}
//
