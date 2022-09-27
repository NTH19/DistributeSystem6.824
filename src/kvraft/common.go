package kvraft

import (
	"fmt"
	"time"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	NIL    = "Nil"

	SUCCESS           = "success"
	NETWORK_FAILURE   = "network too log"
	SERVER_TIMEOUT    = "server too long"
	WRONG_LEADER      = "Wrr leader"
	FAILED_REQUEST    = "failed"
	DUPLICATE_REQUEST = "dup"
)

const CLIENT_REQUEST_INTERVAL = 100 * time.Millisecond
const APPLY_TIMEOUT = 500 * time.Millisecond

type ClerkInfo struct {
	Uid string
	Seq int64
}

type RaftRequest struct {
	Key    string
	Value  string
	OpType string
	ClerkInfo
}

type RaftResponse struct {
	Value   string
	RPCInfo string
}

type PutAppendRequest struct {
	Key    string
	Value  string
	OpType string
	ClerkInfo
}

type PutAppendResponse struct {
	Key    string
	OpType string
	Value  string
	ClerkInfo
	RPCInfo string
}

type GetRequest struct {
	Key string
	ClerkInfo
}

type GetResponse struct {
	Key     string
	Value   string
	RPCInfo string
	ClerkInfo
}

func (c ClerkInfo) String() string {
	return fmt.Sprintf("[%s|SEQ-%d]", c.Uid, c.Seq)
}

func (r PutAppendRequest) String() string {
	return r.ClerkInfo.String() + fmt.Sprintf("[%s K-%s V-%s]", r.OpType, r.Key, r.Value)
}

func (r PutAppendResponse) String() string {
	return r.ClerkInfo.String() + fmt.Sprintf("[%s K-%s %s]", r.OpType, r.Key, r.RPCInfo)
}

func (r GetRequest) String() string {
	return r.ClerkInfo.String() + fmt.Sprintf("[Get K-%s]", r.Key)
}

func (r GetResponse) String() string {
	return r.ClerkInfo.String() + fmt.Sprintf("[Get K-%s %s]", r.Key, r.RPCInfo)
}
