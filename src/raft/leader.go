package raft
//
//
//import (
//	"time"
//)
//
//func  (rf *Raft) DoLeaderJob (){
//	/*
//		Upon election: send initial empty AppendEntries RPCs
//		(heartbeat) to each server; repeat during idle periods to
//		prevent election timeouts (§5.2)
//		• If command received from client: append entry to local log,
//		respond after entry applied to state machine (§5.3)
//		• If last log index ≥ nextIndex for a follower: send
//		AppendEntries RPC with log entries starting at nextIndex
//		• If successful: update nextIndex and matchIndex for
//		follower (§5.3)
//		• If AppendEntries fails because of log inconsistency:
//		decrement nextIndex and retry (§5.3)
//		• If there exists an N such that N > commitIndex, a majority
//		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
//		set commitIndex = N (§5.3, §5.4).
//
//	*/
//
//	/*
//		The leader maintains a nextIndex for each follower,
//		which is the index of the next log entry the leader will
//		send to that follower.
//	*/
//	go func(){
//
//		for{
//
//			_, isLeader := rf.GetState()
//
//			if  isLeader == false {
//				// if not leader, wait
//				rf.mu.Lock()
//				//DPrintf("[LeaderAction] Server %d: Not leader, wait here, term is %d \n", rf.me, term)
//				rf.mu.Unlock()
//
//				// When a leader first comes to power,
//				// it initializes all nextIndex values to the index just after the
//				// last one in its log (11 in Figure 7).
//				//rf.mu.Lock()
//				//for i:=0;i<len(rf.peers);i++{
//				//	//(initialized to leader last log index + 1)
//				//	rf.NextIndex[i] = len(rf.log)-1+1
//				//	//(initialized to 0, increases monotonically)
//				//	rf.MatchIndex[i] = 0
//				//}
//				//rf.mu.Unlock()
//				// send hb
//				//go rf.SendHeartBeat()
//				continue
//				//DPrintf("[LeaderAction]: server %d Awake from sleep, now Im leader!\n", rf.me)
//				//spew.Printf("[AppendEntries]: leader's log are :\n %v \n", rf.me,  rf.log)
//
//			} else {
//				time.Sleep(time.Duration(100) * time.Millisecond)
//				go rf.SendHeartBeat()
//			}
//			time.Sleep(10 * time.Millisecond)
//		}
//	}()
//}
//
//func (rf *Raft) SendHeartBeat(){
//	// update lastSendTime
//	rf.mu.Lock()
//	rf.lastSendTime = time.Now().UnixNano()
//	rf.mu.Unlock()
//	rf.SendAppendEntriesToAll("HeartBeat")
//}