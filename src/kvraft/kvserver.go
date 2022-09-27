package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	lockName string
	lockTime time.Time

	maxraftstate int
	lastIdx      int

	outChan   map[int]map[int]chan RaftResponse
	clientSeq map[string]int64
	state     map[string]string
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func init() {
	labgob.Register(GetRequest{})
	labgob.Register(GetResponse{})

	labgob.Register(PutAppendRequest{})
	labgob.Register(PutAppendResponse{})

	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})

	labgob.Register(raft.RequestVoteRequest{})
	labgob.Register(raft.RequestVoteResponse{})

	labgob.Register(raft.InstallSnapshotRequest{})
	labgob.Register(raft.InstallSnapshotResponse{})

	labgob.Register(RaftRequest{})
	labgob.Register(RaftResponse{})
}

// StartKVServer must return quickly, so it should start goroutines
// for any long-running work.
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.state = make(map[string]string)
	kv.clientSeq = make(map[string]int64)
	kv.outChan = make(map[int]map[int]chan RaftResponse)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Loop()

	return kv
}

func (kv *KVServer) deserializeState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clients map[string]int64
	var state map[string]string
	var idx int
	if d.Decode(&clients) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&idx) != nil {
		panic("BAD KV PERSIST")
	} else {
		kv.clientSeq = clients
		kv.state = state
		kv.lastIdx = idx
	}
	//fmt.Printf("[KV %d] Loading Snapshot, applied-idx=%d\n", kv.me, idx)
}
func (kv *KVServer) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSeq)
	e.Encode(kv.state)
	e.Encode(kv.lastIdx)
	return w.Bytes()
}
func (kv *KVServer) removeOutchan(idx, term int) {
	kv.lock("remove chan")
	delete(kv.outChan[idx], term)
	kv.unlock()
}

func (kv *KVServer) cleanOutchan(uptoIdx int) {
	for idx, v := range kv.outChan {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- RaftResponse{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.outChan, idx)
		}
	}
}

func (kv *KVServer) Loop() {

	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

main:
	for !kv.killed() {

		msg := <-kv.applyCh

		kv.lock("kv Loop")
		kv.lastIdx = msg.CommandIndex

		if msg.SnapshotValid {
			//kv.Log("收到快照{...=>[%d|%d]", msg.SnapshotIndex, msg.SnapshotTerm)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.deserializeState(msg.Snapshot)
				kv.cleanOutchan(msg.SnapshotIndex)
			}
			kv.unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(RaftRequest)
		//kv.Log("收到日志[%d|%d] {%+v}", idx, term, msg.Command)

		// Dup
		seq := kv.clientSeq[r.Uid]
		if r.OpType != GET && r.Seq <= seq {
			if ch, ok := kv.outChan[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: DUPLICATE_REQUEST}
				delete(kv.outChan[idx], term)
			}
			kv.unlock()
			continue main
		}

		kv.clientSeq[r.Uid] = r.Seq

		switch r.OpType {
		case GET:
			val := kv.state[r.Key]
			if ch, ok := kv.outChan[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.outChan[idx], term)
			}

		case PUT:
			kv.state[r.Key] = r.Value
			val := kv.state[r.Key]
			if ch, ok := kv.outChan[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.outChan[idx], term)
			}

		case APPEND:
			kv.state[r.Key] = kv.state[r.Key] + r.Value
			val := kv.state[r.Key]
			if ch, ok := kv.outChan[idx][term]; ok {
				ch <- RaftResponse{RPCInfo: SUCCESS, Value: val}
				delete(kv.outChan[idx], term)
			}
		}

		if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
			stateBytes := kv.serializeState()
			kv.cleanOutchan(msg.CommandIndex)
			kv.rf.Snapshot(msg.CommandIndex, stateBytes)
		}

		kv.unlock()
	}
}

func (kv *KVServer) InputCommand(req RaftRequest) (resp RaftResponse) {

	kv.lock("try apply")
	var idx, term int
	var ok bool
	switch req.OpType {
	case NIL:
		idx, term, ok = kv.rf.Start(nil)
	default:
		idx, term, ok = kv.rf.Start(req)
	}

	if !ok {
		kv.unlock()
		resp.RPCInfo = WRONG_LEADER
		return
	}

	ch := make(chan RaftResponse)
	if mm := kv.outChan[idx]; mm == nil {
		mm = make(map[int]chan RaftResponse)
		kv.outChan[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.unlock()

	t := time.NewTimer(APPLY_TIMEOUT)
	defer t.Stop()
	select {
	case resp = <-ch:
		kv.removeOutchan(idx, term)
		return
	case <-t.C:
		kv.removeOutchan(idx, term)
		resp.RPCInfo = SERVER_TIMEOUT
		return
	}
}
func (kv *KVServer) Get(args *GetRequest, reply *GetResponse) {
	//defer func() {
	//	kv.Debug("Get RPC returns, %+v", *reply)
	//}()
	req := RaftRequest{
		Key:       args.Key,
		ClerkInfo: args.ClerkInfo,
		OpType:    GET,
	}

	resp := kv.InputCommand(req)

	reply.Key = args.Key
	reply.ClerkInfo = args.ClerkInfo
	reply.Value = resp.Value
	reply.RPCInfo = resp.RPCInfo
}

func (kv *KVServer) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	//defer func() {
	//	kv.Debug("PutAppend RPC returns, %+v", *reply)
	//}()
	req := RaftRequest{
		Key:       args.Key,
		Value:     args.Value,
		ClerkInfo: args.ClerkInfo,
		OpType:    args.OpType,
	}

	resp := kv.InputCommand(req)

	reply.Key = args.Key
	reply.ClerkInfo = args.ClerkInfo
	reply.OpType = args.OpType
	reply.Value = resp.Value
	reply.RPCInfo = resp.RPCInfo
}
