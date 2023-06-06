package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId     int64
	ClientId int64
	Index    int
	Term     int
	OpType   string
	OpKey    string
	OpValue  string

}

type KVServer struct {
	mu      sync.Mutex
	pmu      sync.Mutex

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sequenceMapper map[int64]int64
	requestMapper  map[int]chan Op
	kvStore        map[string]string
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	var isLeader bool
	clientOp := Op{OpType: "Get", OpKey: args.Key, OpId: args.SequenceId, ClientId: args.ClientId}
	clientOp.Index, clientOp.Term, isLeader = kv.rf.Start(clientOp)
	kv.pmu.Lock()
	//DPrintf("Get  key:%v  seqId:%d  clientID:%d" ,args.Key, args.SequenceId,args.ClientId)
	kv.pmu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//set connection
	ch := kv.getChannel(clientOp.Index)

	//delete connection
	defer func() {
		kv.mu.Lock()
		delete(kv.requestMapper, clientOp.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(500 * time.Millisecond)

	defer timer.Stop()
	select {
	case op := <-ch:
		kv.mu.Lock()
		opTerm := op.Term
		kv.mu.Unlock()
		if clientOp.Term != opTerm {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvStore[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var isLeader bool

	clientOp := Op{OpType: args.Op, OpKey: args.Key, OpValue: args.Value, OpId: args.SequenceId, ClientId: args.ClientId}
	kv.pmu.Lock()
	//DPrintf("Put append Op:%v key:%v value:%v seqId:%d clientID:%d" ,args.Op,args.Key,args.Value, args.SequenceId,args.ClientId)
	kv.pmu.Unlock()
	clientOp.Index, clientOp.Term, isLeader = kv.rf.Start(clientOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}


	// leader is found
	ch := kv.getChannel(clientOp.Index)

	defer func() {
		kv.mu.Lock()
		delete(kv.requestMapper, clientOp.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case op := <-ch:
		kv.mu.Lock()
		opTerm := op.Term
		kv.mu.Unlock()
		if clientOp.Term != opTerm {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}


func (kv *KVServer) getChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.requestMapper[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.requestMapper[index] = ch
	}
	return ch
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.sequenceMapper = make(map[int64]int64)
	kv.requestMapper = make(map[int]chan Op)
	kv.kvStore = make(map[string]string)

	go kv.serverMonitor()

	return kv
}
func (kv *KVServer) serverMonitor() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			index := msg.CommandIndex
			term := msg.CommandTerm
			op := msg.Command.(Op)
			kv.mu.Lock()
			sequenceInMapper, hasSequence := kv.sequenceMapper[op.ClientId]
			op.Term = term
			kv.pmu.Lock()
			//DPrintf("~~~~~get command %v sID:%v  CID:%d   %d  ",op.OpType,op.OpId,op.ClientId,sequenceInMapper)
			kv.pmu.Unlock()
			//qu chong
			if !hasSequence || op.OpId > sequenceInMapper {
				kv.pmu.Lock()
				//DPrintf("get command %v",op.OpType)
				kv.pmu.Unlock()
				switch op.OpType {
				case "Put":
					kv.kvStore[op.OpKey] = op.OpValue
				case "Append":
					kv.kvStore[op.OpKey] += op.OpValue
				}
				kv.sequenceMapper[op.ClientId] = op.OpId
				kv.pmu.Lock()
				//DPrintf("now key:%v value:%v",op.OpKey,kv.kvStore[op.OpKey])
				kv.pmu.Unlock()
			}
			kv.mu.Unlock()
			// send message to op chan
			kv.getChannel(index) <- op
		}
	}
}
