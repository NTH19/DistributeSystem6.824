package shardkv

import (
	"fmt"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) lock(namespace string) {
	kv.mu.Lock()
	kv.lockname = namespace
	kv.locktime = time.Now()
}

func (kv *ShardKV) unlock() {
	if d := time.Since(kv.locktime); d >= LOCK_TIMEOUT {
		fmt.Println("UNLOCK:", kv.lockname, "too long, cost: ", d)
	}
	kv.mu.Unlock()
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	//Debug("========"+SRV_FORMAT+"CRASHED========", kv.gid, kv.me)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
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

	labgob.Register(SingleShardData{})
	labgob.Register(SingleShardInfo{})

	labgob.Register(GeneralInput{})
	labgob.Register(GeneralOutput{})
	labgob.Register(shardctrler.JoinArgs{})
	labgob.Register(shardctrler.JoinReply{})

	labgob.Register(shardctrler.LeaveArgs{})
	labgob.Register(shardctrler.LeaveReply{})

	labgob.Register(shardctrler.MoveArgs{})
	labgob.Register(shardctrler.MoveReply{})

	labgob.Register(shardctrler.QueryArgs{})
	labgob.Register(shardctrler.QueryResponse{})

	labgob.Register(shardctrler.Config{})
	labgob.Register(map[int][]string{})
	labgob.Register([]int{})

	labgob.Register(shardctrler.MoveAc{})
}
