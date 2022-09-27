package kvraft

import (
	"strconv"
	"time"

	"6.824/labrpc"
)

var (
	KVClientGlobalId int64
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	size    int
	ClerkInfo
	recentLeader int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.Uid = GenerateClerkId()
	ck.size = len(ck.servers)
	// fmt.Println("NEW CLIENT", ck.Uid)
	return ck
}
func GenerateClerkId() string {
	KVClientGlobalId++
	return strconv.FormatInt(KVClientGlobalId, 10)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	ck.Seq++

	req := GetRequest{
		Key: key,
		ClerkInfo: ClerkInfo{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		for range ck.servers {
			//ck.Log("开始Get%+v [KV %d]", req, i)
			var resp GetResponse
			ck.servers[i].Call("KVServer.Get", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				return resp.Value
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.Seq++

	req := PutAppendRequest{
		Key:    key,
		Value:  value,
		OpType: op,
		ClerkInfo: ClerkInfo{
			Uid: ck.Uid,
			Seq: ck.Seq,
		},
	}

	i := ck.recentLeader
	for {
		// try each known server.
		for range ck.servers {
			//ck.Log("开始PutAppend%+v [KV %d]", req, i)
			var resp GetResponse
			ck.servers[i].Call("KVServer.PutAppend", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				ck.recentLeader = i
				return
			} else if resp.RPCInfo == DUPLICATE_REQUEST {
				return
			}
			i = (i + 1) % ck.size
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
