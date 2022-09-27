package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	LOAD_SHARD    = "load shard"
	CLEAN_SHARD   = "clean shard"
	UPDATE_CONFIG = "chang config"
)

const (
	SUCCESS           = "success"
	FAILED_REQUEST    = "fail request"
	DUPLICATE_REQUEST = "dup request"
	WRONG_LEADER      = "wrong leader"
	WRONG_GROUP       = "wrong group"
	APPLY_TIMEOUT     = "apply timeout"
)

const (
	SHARD_OPERATION_INTERVAL = 100 * time.Millisecond
	CONFIG_LISTEN_INTERVAL   = 100 * time.Millisecond // 监听多集群配置变化的间隔时间
	CONFIG_LISTEN_TIMEOUT    = 1000 * time.Millisecond
	INTERNAL_TIMEOUT         = 500 * time.Millisecond // 内部逻辑处理最大允许时长，超时后RPC将提前返回
	LOCK_TIMEOUT             = 200 * time.Millisecond
)

const (
	NOTINCHARGE = iota
	INCHARGE
	RECEIVING
	TRANSFERRING
)

type ClientInfo struct {
	Uid int64
	Seq int64
}

type GeneralInput struct {
	Key    string
	Value  string
	OpType string
	ClientInfo
	Input interface{}
}

type GeneralOutput struct {
	Value   string
	Output  interface{}
	RPCInfo string
}

type PutAppendRequest struct {
	Key    string
	Value  string
	OpType string
	ClientInfo
}

type PutAppendResponse struct {
	Key        string // redundant info
	OpType     string // redundant info
	Value      string // redundant info
	ClientInfo        // redundant info
	RPCInfo    string
}

type GetRequest struct {
	Key string
	ClientInfo
}

type GetResponse struct {
	Key     string // redundant info
	Value   string
	RPCInfo string
	ClientInfo
}

type ReceiveShardRequest struct {
	SingleShardData
}

type ReceiveShardResponse struct {
	SingleShardInfo // redudant info
	RPCInfo         string
}

type SingleShardInfo struct {
	SenderGid int
	ConfigIdx int
	Shard     int
}

type SingleShardData struct {
	SingleShardInfo
	State   map[string]string
	Clients map[int64]int64
}
