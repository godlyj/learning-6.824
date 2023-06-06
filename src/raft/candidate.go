package raft
//
//import (
//	"time"
//)
//
//func (rf *Raft) DoCandidateJob() {
//	rf.mu.Lock()
//	rf.state = Candidate
//	rf.CurrentTerm+=1
//	rf.votedFor = rf.me
//	rf.LastHeartBeatTime = time.Now()
//	rf.mu.Unlock()
//	//rf.persist()
//
//	granted := 1
//	go rf.BoardRequestVote(&granted)
//
//}
//
//
//func (rf *Raft) BoardRequestVote(granted *int){
//	args := new(RequestVoteArgs)
//	rf.mu.Lock()
//	args.Term = rf.CurrentTerm
//	args.CandidateId = rf.me
//	rf.mu.Unlock()
//
//	for i := 0; i < len(rf.peers); i++ {
//		if i == rf.me {
//			continue
//		}
//		k := i
//		rf.mu.Lock()
//		//DPrintf("[CandidateAction] : %d send to %d Term %d \n", rf.me, k, rf.CurrentTerm)
//		rf.mu.Unlock()
//		go rf.requestVote(granted, args, k)
//	}
//}
//
//func (rf *Raft) requestVote(vote *int, args *RequestVoteArgs, k int) {
//
//	reply := new(RequestVoteReply)
//
//	ok := rf.sendRequestVote(k, args, reply)
//
//	if ok == false {
//		//DPrintf("[CandidateAction] : %d sendRequestVote to %d failed \n", rf.me, k)
//		return
//	}
//
//	// receiver implementation::
//	/*
//		1. Reply false if term < currentTerm (§5.1)
//		2. If votedFor is null or candidateId, and candidate’s log is at
//		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
//	*/
//	if reply.VoteGranted == false {
//		rf.mu.Lock()
//		// If RPC request or response contains term T > currentTerm:
//		// set currentTerm = T, convert to follower (§5.1)
//		if reply.Term > rf.CurrentTerm {
//			rf.BecomeFollower(reply.Term)
//			rf.mu.Unlock()
//			return
//		} else {
//			rf.mu.Unlock()
//		}
//
//	} else if reply.VoteGranted == true {
//		rf.mu.Lock()
//		*vote += 1
//		if rf.state != Leader {
//			if *vote >= len(rf.peers)/2+1 {
//				rf.state = Leader
//				// lock should not include Broadcast
//				rf.persist()
//			}
//		}
//		rf.mu.Unlock()
//	}
//}
