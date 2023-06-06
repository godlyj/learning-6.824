package raft
//
//import (
//	"math/rand"
//	"time"
//)
//
//func (rf *Raft) BecomeFollower(term int) {
//	rf.CurrentTerm = term
//	rf.state = Follower
//	// goes to un-voted stats
//	rf.votedFor = -1
//	//rf.persist()
//}
//
//func (rf *Raft) DoFollowerJob() {
//	go func(){
//		for{
//			_, isLeader := rf.GetState()
//			if isLeader {
//				rf.mu.Lock()
//				//DPrintf("[FollowerAction]: server %d become leader at term %d, blocking...\n", rf.me, rf.CurrentTerm)
//				continue
//				rf.mu.Unlock()
//			} else {
//				rf.mu.Lock()
//				electionTimeout := 500 + rand.Intn(200)
//				startTime := time.Now()
//				time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
//				//DPrintf("[FollowerAction]: server %d  curreut time reaches %d \n",
//				//	rf.me, int(elapseTime/int64(time.Millisecond)))
//				if rf.LastHeartBeatTime.Before(startTime) {
//					go rf.DoCandidateJob()
//				}
//				rf.mu.Unlock()
//			}
//			time.Sleep(time.Millisecond * 10)
//
//		}
//	}()
//}