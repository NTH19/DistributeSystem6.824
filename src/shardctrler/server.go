package shardctrler

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})

	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})

	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})

	labgob.Register(QueryArgs{})
	labgob.Register(QueryResponse{})

	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})

	labgob.Register(raft.RequestVoteRequest{})
	labgob.Register(raft.RequestVoteResponse{})

	labgob.Register(Input{})
	labgob.Register(Output{})

	labgob.Register(map[int][]string{})
	labgob.Register(MoveAc{})
}

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32
	lockname  string
	locktime  time.Time
	outChans  map[int]map[int]chan Output
	clientSeq map[int64]int64
	configs   []Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	in := Input{
		OpType:     JOIN,
		ClientInfo: args.ClientInfo,
		Input:      args.Servers,
	}
	out := sc.InputCommand(in)
	reply.RPCInfo = out.RPCInfo
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	in := Input{
		OpType:     LEAVE,
		ClientInfo: args.ClientInfo,
		Input:      args.GIDs,
	}
	out := sc.InputCommand(in)
	reply.RPCInfo = out.RPCInfo
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	in := Input{
		OpType:     MOVE,
		ClientInfo: args.ClientInfo,
		Input:      args.MoveAc,
	}
	out := sc.InputCommand(in)
	reply.RPCInfo = out.RPCInfo
}
func CopyConfig(des, src *Config) {
	des.Num = src.Num

	for i := range des.Shards {
		des.Shards[i] = src.Shards[i]
	}

	des.Groups = make(map[int][]string)

	for k, v := range src.Groups {
		arr := make([]string, len(v))
		copy(arr, v)
		des.Groups[k] = arr
	}
}
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	in := Input{
		OpType:     QUERY,
		ClientInfo: args.ClientInfo,
		Input:      args.Idx,
	}
	out := sc.InputCommand(in)
	if out.RPCInfo != SUCCESS {
		reply.RPCInfo = out.RPCInfo
		return
	}
	src := out.Output.(Config)
	CopyConfig(&reply.Config, &src)
	reply.RPCInfo = out.RPCInfo
}

func (sc *ShardCtrler) InputCommand(req Input) (resp Output) {

	idx, term, ok := sc.rf.Start(req)
	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}

	sc.lock("Input Command")
	ch := make(chan Output, 1)
	if mm := sc.outChans[idx]; mm == nil {
		mm = make(map[int]chan Output)
		sc.outChans[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	sc.unlock()

	t := time.NewTimer(INTERNAL_TIMEOUT)
	select {
	case resp = <-ch:
		sc.removeOutChan(idx, term)
		return
	case <-t.C:
		sc.removeOutChan(idx, term)
		resp.RPCInfo = APPLY_TIMEOUT
		return
	}
}
func (sc *ShardCtrler) removeOutChan(idx, term int) {
	sc.lock("remove OutChan")
	delete(sc.outChans[idx], term)
	sc.unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	//Debug("========"+SRV_FORMAT+"CRASHED========", sc.me)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clientSeq = make(map[int64]int64)
	sc.outChans = make(map[int]map[int]chan Output)

	go sc.Loop()

	return sc
}

func (sc *ShardCtrler) Loop() {

main:
	for !sc.killed() {

		msg := <-sc.applyCh
		sc.lock("Loop")
		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(Input)
		//sc.info("收到日志[%d|%d] %s", idx, term, r.OpType)

		seq := sc.clientSeq[r.Uid]
		if r.OpType != QUERY && r.Seq <= seq {
			if ch, ok := sc.outChans[idx][term]; ok {
				ch <- Output{RPCInfo: DUPLICATE_REQUEST}
				delete(sc.outChans[idx], term)
			}
			sc.unlock()
			continue main
		}

		sc.clientSeq[r.Uid] = r.Seq

		switch r.OpType {
		case JOIN:
			servers := r.Input.(map[int][]string)
			sc.joinGroups(servers)
			if ch, ok := sc.outChans[idx][term]; ok {
				ch <- Output{RPCInfo: SUCCESS}
				delete(sc.outChans[idx], term)
			}

		case LEAVE:
			gids := r.Input.([]int)
			sc.leaveGroups(gids)
			if ch, ok := sc.outChans[idx][term]; ok {
				ch <- Output{RPCInfo: SUCCESS}
				delete(sc.outChans[idx], term)
			}

		case MOVE:
			movable := r.Input.(MoveAc)
			sc.moveOneShard(movable)
			if ch, ok := sc.outChans[idx][term]; ok {
				ch <- Output{RPCInfo: SUCCESS}
				delete(sc.outChans[idx], term)
			}

		case QUERY:
			num := r.Input.(int)
			var config Config
			if num != -1 && num < len(sc.configs) {
				config = sc.configs[num]
			} else {
				config = sc.configs[len(sc.configs)-1]
			}
			if ch, ok := sc.outChans[idx][term]; ok {
				ch <- Output{RPCInfo: SUCCESS, Output: config}
				delete(sc.outChans[idx], term)
			}
		}
		sc.unlock()
	}
}
func (sc *ShardCtrler) joinGroups(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	gids := Heap{}
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
		heap.Push(&gids, g)
	}
	for g, srvs := range servers {
		newConfig.Groups[g] = srvs
		heap.Push(&gids, g)
	}
	reallocSlots(&newConfig, &gids)
	sc.configs = append(sc.configs, newConfig)
}

func reallocSlots(config *Config, gids *Heap) {
	size := gids.Len()
	if size == 0 {
		return
	}
	each, res := NShards/size, NShards%size
	var offset int
	for gids.Len() > 0 {
		g := heap.Pop(gids).(int)
		for i := 0; i < each; i++ {
			config.Shards[offset] = g
			offset++
		}
		if res > 0 {
			res--
			config.Shards[offset] = g
			offset++
		}
	}
}
func (sc *ShardCtrler) leaveGroups(lgids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
	}
	for _, lg := range lgids {
		delete(newConfig.Groups, lg)
	}
	gids := Heap{}
	for g := range newConfig.Groups {
		heap.Push(&gids, g)
	}
	reallocSlots(&newConfig, &gids)
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) moveOneShard(m MoveAc) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}
	for g, srvs := range lastConfig.Groups {
		newConfig.Groups[g] = srvs
	}
	for s := range lastConfig.Shards {
		newConfig.Shards[s] = lastConfig.Shards[s]
	}
	newConfig.Shards[m.Shard] = m.GID
	sc.configs = append(sc.configs, newConfig)
}
