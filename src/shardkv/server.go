package shardkv

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu       sync.Mutex
	locktime time.Time
	lockname string

	me       int
	gid      int
	scc      *shardctrler.Clerk
	make_end func(string) *labrpc.ClientEnd

	clientSeq   [shardctrler.NShards]map[int64]int64
	shardedData [shardctrler.NShards]map[string]string

	conf shardctrler.Config

	shardState [shardctrler.NShards]int

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int
	dead         int32

	outChans map[int]map[int]chan GeneralOutput
}

// Get returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (kv *ShardKV) Get(args *GetRequest, reply *GetResponse) {
	defer func() {
		kv.lock("Get RPC returns")
		kv.unlock()
	}()

	in := GeneralInput{
		OpType:     GET,
		Key:        args.Key,
		ClientInfo: args.ClientInfo,
	}

	out := kv.InputCommand(in)
	reply.Key = args.Key
	reply.ClientInfo = args.ClientInfo
	reply.RPCInfo = out.RPCInfo
	reply.Value = out.Value
}

// PutAppend returns: APPLY_TIMEOUT/FAILED_REQUEST/SUCCESS/DUPLICATE_REQUEST
func (kv *ShardKV) PutAppend(args *PutAppendRequest, reply *PutAppendResponse) {
	defer func() {
		kv.lock("PutAppend RPC returns")
		kv.unlock()
	}()

	in := GeneralInput{
		OpType:     args.OpType,
		Key:        args.Key,
		Value:      args.Value,
		ClientInfo: args.ClientInfo,
	}

	out := kv.InputCommand(in)
	reply.Key = args.Key
	reply.ClientInfo = args.ClientInfo
	reply.OpType = args.OpType
	reply.RPCInfo = out.RPCInfo
	reply.Value = out.Value
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardRequest, reply *ReceiveShardResponse) {
	//defer func() {
	//kv.lock("ReceiveShard RPC returns")
	//kv.info("ReceiveShard RPC returns, %+v", *reply)
	//kv.unlock()
	//}()

	in := GeneralInput{
		OpType: LOAD_SHARD,
		Input:  args.SingleShardData,
	}
	out := kv.InputCommand(in)
	reply.SingleShardInfo = args.SingleShardInfo
	reply.RPCInfo = out.RPCInfo
}
func (kv *ShardKV) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSeq)
	e.Encode(kv.shardedData)
	e.Encode(kv.conf)
	e.Encode(kv.shardState)
	return w.Bytes()
}
func (kv *ShardKV) deserializeState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clientSeq [shardctrler.NShards]map[int64]int64
	var shardedData [shardctrler.NShards]map[string]string
	var conf shardctrler.Config
	var shardsState [shardctrler.NShards]int
	if d.Decode(&clientSeq) != nil ||
		d.Decode(&shardedData) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&shardsState) != nil {
		panic("bad deserialization")
	} else {
		kv.clientSeq = clientSeq
		kv.shardedData = shardedData
		kv.conf = conf
		kv.shardState = shardsState
	}
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.scc = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.outChans = make(map[int]map[int]chan GeneralOutput)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.clientSeq[i] = make(map[int64]int64)
		kv.shardedData[i] = make(map[string]string)
	}

	data := kv.rf.LastestSnapshot().Data
	kv.deserializeState(data)

	go kv.Loop()
	go kv.ListenConfigLoop()
	for i := 0; i < shardctrler.NShards; i++ {
		go kv.shardLoop(i)
	}

	//Debug("========"+SRV_FORMAT+"STARTED========", kv.gid, kv.me)
	return kv
}

func (kv *ShardKV) Loop() {

main:
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.lock("shard kv loop")

		if msg.SnapshotValid {
			kv.tryLoadSnapshot(&msg)
			kv.unlock()
			continue main
		}

		idx, term := msg.CommandIndex, msg.CommandTerm
		r := msg.Command.(GeneralInput)
		//kv.info("收到日志[%d|%d] %s", idx, term, r.OpType)

		switch r.OpType {
		case GET, PUT, APPEND:
			if succeed := kv.ShardExecuteCommand(idx, term, &r); succeed {
				kv.conSaveSnapshot(idx, term)
			}
		case UPDATE_CONFIG:
			if succeed := kv.loadConfig(&r); succeed {
				kv.saveSnapshot(idx, term)
			}
		case LOAD_SHARD:
			if succeed := kv.loadShard(idx, term, &r); succeed {
				kv.saveSnapshot(idx, term)
			}

		case CLEAN_SHARD:
			if succeed := kv.cleanShard(&r); succeed {
				kv.saveSnapshot(idx, term)
			}

		default:
			//kv.error("invalid operation type")
			panic("invalid operation type")
		}

		kv.unlock()
	}
}
func (kv *ShardKV) tryLoadSnapshot(msg *raft.ApplyMsg) (succeed bool) {
	kv.lockname = "try load snapshot"
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		succeed = true
		kv.deserializeState(msg.Snapshot)
		kv.cleanOutChans(msg.SnapshotIndex)
	}
	return
}
func (kv *ShardKV) saveSnapshot(idx, term int) (succeed bool) {
	kv.lockname = "save snapshot"
	if kv.maxraftstate == -1 {
		return
	}
	succeed = true
	stateBytes := kv.serializeState()
	kv.cleanOutChans(idx)
	kv.rf.Snapshot(idx, stateBytes)
	return
}
func (kv *ShardKV) conSaveSnapshot(idx, term int) (succeed bool) {
	kv.lockname = "condition save snapshot"
	if kv.maxraftstate != -1 && kv.rf.ShouldSnapshot(kv.maxraftstate) {
		succeed = true
		kv.saveSnapshot(idx, term)
	}
	return
}

func (kv *ShardKV) loadShard(idx, term int, r *GeneralInput) (success bool) {
	kv.lockname = "try load shard"
	data := r.Input.(SingleShardData)
	s := data.Shard

	if data.ConfigIdx >= kv.conf.Num {
		if ch, ok := kv.outChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.outChans[idx], term)
		}
		return
	}

	if data.ConfigIdx < kv.conf.Num-1 || kv.shardState[s] == INCHARGE {
		if ch, ok := kv.outChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.outChans[idx], term)
		}
		return
	}

	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		//kv.error("invalid shardState[%d]=%d", s, kv.shardState[s])
		panic("invalid shardState")
		return
	}

	success = true

	kv.shardedData[s] = make(map[string]string)
	for k, v := range data.State {
		kv.shardedData[s][k] = v
	}
	kv.clientSeq[s] = make(map[int64]int64)
	for k, v := range data.Clients {
		kv.clientSeq[s][k] = v
	}
	kv.shardState[s] = INCHARGE
	if ch, ok := kv.outChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCESS}
		delete(kv.outChans[idx], term)
	}
	return
}

func (kv *ShardKV) cleanShard(r *GeneralInput) (success bool) {
	kv.lockname = "try clean shard"
	data := r.Input.(SingleShardInfo)
	s := data.Shard

	if kv.shardState[s] == TRANSFERRING && data.ConfigIdx == kv.conf.Num-1 {
		success = true
		//defer kv.info("成功清理分片%+v", data)
		kv.shardState[s] = NOTINCHARGE
		kv.clientSeq[s] = make(map[int64]int64)
		kv.shardedData[s] = make(map[string]string)
	}
	return
}
func (kv *ShardKV) ShardExecuteCommand(idx, term int, r *GeneralInput) (succeed bool) {
	kv.lockname = "shard  execute command" + r.OpType
	s := key2shard(r.Key)
	if kv.shardState[s] == NOTINCHARGE || kv.shardState[s] == TRANSFERRING {
		if ch, ok := kv.outChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: WRONG_GROUP}
			delete(kv.outChans[idx], term)
		}
		return
	}
	if kv.shardState[s] == RECEIVING {
		if ch, ok := kv.outChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: FAILED_REQUEST}
			delete(kv.outChans[idx], term)
		}
		return
	}
	if r.OpType != GET && r.Seq <= kv.clientSeq[s][r.Uid] {
		if ch, ok := kv.outChans[idx][term]; ok {
			ch <- GeneralOutput{RPCInfo: DUPLICATE_REQUEST}
			delete(kv.outChans[idx], term)
		}
		return
	}
	succeed = true
	kv.clientSeq[s][r.Uid] = r.Seq
	var val string
	switch r.OpType {
	case GET:
		val = kv.shardedData[s][r.Key]

	case PUT:
		kv.shardedData[s][r.Key] = r.Value
		val = kv.shardedData[s][r.Key]

	case APPEND:
		kv.shardedData[s][r.Key] = kv.shardedData[s][r.Key] + r.Value
		val = kv.shardedData[s][r.Key]
	}
	//may delete due to too time waste
	if ch, ok := kv.outChans[idx][term]; ok {
		ch <- GeneralOutput{RPCInfo: SUCCESS, Value: val}
		delete(kv.outChans[idx], term)
	}
	return
}

func (kv *ShardKV) loadConfig(r *GeneralInput) (success bool) {
	kv.lockname = "try load config"
	config := r.Input.(shardctrler.Config)
	if config.Num != kv.conf.Num+1 {
		return
	}
	for s := range kv.shardState {
		if kv.shardState[s] != INCHARGE && kv.shardState[s] != NOTINCHARGE {
			return
		}
	}
	success = true
	//defer kv.info("成功应用[Config %d]", config.Num)
	var oldConfig shardctrler.Config
	shardctrler.CopyConfig(&oldConfig, &kv.conf)

	if oldConfig.Num == 0 {
		for s, ngid := range config.Shards {
			if ngid == kv.gid {
				kv.shardState[s] = INCHARGE
			}
		}
		shardctrler.CopyConfig(&kv.conf, &config)
		return
	}

	for s, ngid := range config.Shards {
		switch kv.shardState[s] {
		case NOTINCHARGE:
			if ngid == kv.gid {
				kv.shardState[s] = RECEIVING
			}

		case INCHARGE:
			if ngid != kv.gid {
				kv.shardState[s] = TRANSFERRING
			}

		default:
			panic("invalid shardState")
			//kv.error("invalid shardState[%d]=%d", s, kv.shardState[s])
		}
	}

	shardctrler.CopyConfig(&kv.conf, &config)
	return
}
func (kv *ShardKV) ListenConfigLoop() {
outer:
	for !kv.killed() {
		time.Sleep(CONFIG_LISTEN_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		t := time.NewTimer(CONFIG_LISTEN_TIMEOUT)
		kv.lock("ListenConfigLoop")
		c := kv.conf.Num
		kv.unlock()
		ch := make(chan shardctrler.Config)
		var config shardctrler.Config
		go func(configNum int) { ch <- kv.scc.Query(configNum) }(c + 1)
		select {
		case <-t.C:
			//Debug(SRV_FORMAT+"监听配置超时", kv.gid, kv.me)
			continue outer
		case config = <-ch:
		}

		kv.lock("config listen")
		needChange := config.Num == kv.conf.Num+1
		kv.unlock()
		if needChange {
			// cp := shardctrler.Config{}
			// shardctrler.CopyConfig(&cp, &config)
			kv.rf.Start(GeneralInput{
				OpType: UPDATE_CONFIG,
				Input:  config,
			})
		}
	}
}

func (kv *ShardKV) shardLoop(s int) {
outer:
	for !kv.killed() {
		time.Sleep(SHARD_OPERATION_INTERVAL)
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		kv.lock("shard operation" + strconv.Itoa(s))
		if kv.shardState[s] == TRANSFERRING {
			state := make(map[string]string)
			clients := make(map[int64]int64)
			gid := kv.conf.Shards[s]
			srvs := make([]string, len(kv.conf.Groups[gid]))
			copy(srvs, kv.conf.Groups[gid])
			for k, v := range kv.shardedData[s] {
				state[k] = v
			}
			for k, v := range kv.clientSeq[s] {
				clients[k] = v
			}
			data := SingleShardData{
				SingleShardInfo: SingleShardInfo{
					Shard:     s,
					ConfigIdx: kv.conf.Num - 1,
					SenderGid: kv.gid,
				},
				State:   state,
				Clients: clients,
			}
			req := ReceiveShardRequest{SingleShardData: data}
			kv.unlock()
			for _, srvi := range srvs {
				var resp ReceiveShardResponse
				srv := kv.make_end(srvi)
				srv.Call("ShardKV.ReceiveShard", &req, &resp)
				if resp.RPCInfo == SUCCESS || resp.RPCInfo == DUPLICATE_REQUEST {
					r := GeneralInput{
						OpType: CLEAN_SHARD,
						Input:  data.SingleShardInfo,
					}
					kv.InputCommand(r)
					continue outer
				}
			}
		} else {
			kv.unlock()
		}
	}
}

func (kv *ShardKV) InputCommand(req GeneralInput) (resp GeneralOutput) {
	idx, term, ok := kv.rf.Start(req)

	if !ok {
		resp.RPCInfo = WRONG_LEADER
		return
	}
	kv.lock("try apply" + req.OpType)

	ch := make(chan GeneralOutput, 1)
	if mm := kv.outChans[idx]; mm == nil {
		mm = make(map[int]chan GeneralOutput)
		kv.outChans[idx] = mm
		mm[term] = ch
	} else {
		mm[term] = ch
	}
	kv.unlock()

	t := time.NewTimer(INTERNAL_TIMEOUT)
	select {
	case resp = <-ch:
		kv.removeOutChan(idx, term)
		return
	case <-t.C:
		kv.removeOutChan(idx, term)
		resp.RPCInfo = APPLY_TIMEOUT
		return
	}
}
func (kv *ShardKV) removeOutChan(idx, term int) {
	kv.lock("remove out chan lock")
	delete(kv.outChans[idx], term)
	kv.unlock()
}

func (kv *ShardKV) cleanOutChans(uptoIdx int) {
	for idx, v := range kv.outChans {
		if idx <= uptoIdx {
			for term, vv := range v {
				vv <- GeneralOutput{RPCInfo: FAILED_REQUEST}
				delete(v, term)
			}
			delete(kv.outChans, idx)
		}
	}
}
