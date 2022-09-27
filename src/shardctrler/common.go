package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const (
	NShards          = 10                     // number of shards
	INTERNAL_TIMEOUT = 500 * time.Millisecond // allowed duration for processing request
	LOCK_TIMEOUT     = 200 * time.Millisecond // allowed duration inside a lock
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// operation type
const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
	NIL   = "NIL"
)

// rpc information
const (
	SUCCESS           = "成功请求"
	APPLY_TIMEOUT     = "请求超时"
	WRONG_LEADER      = "错误领袖"
	FAILED_REQUEST    = "失败请求"
	DUPLICATE_REQUEST = "重复请求"
)

type Err string
type ClientInfo struct {
	Uid int64
	Seq int64
}
type QueryResponse struct {
	Config
	RPCInfo string
}

type Input struct {
	OpType string
	ClientInfo
	Input interface{}
}

type Output struct {
	Output  interface{}
	RPCInfo string
}
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientInfo
}

type JoinReply struct {
	RPCInfo string
}

type LeaveArgs struct {
	GIDs []int
	ClientInfo
}

type LeaveReply struct {
	RPCInfo string
}
type MoveAc struct {
	Shard int
	GID   int
}
type MoveArgs struct {
	MoveAc
	ClientInfo
}

type MoveReply struct {
	RPCInfo string
}

type QueryArgs struct {
	Idx int
	ClientInfo
}

type QueryReply struct {
	Config
	RPCInfo string
}
