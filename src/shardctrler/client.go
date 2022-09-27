package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	size         int
	recentLeader int32
	ClientInfo
}

const CLIENT_REQUEST_INTERVAL = 100 * time.Millisecond

var (
	ctrlerClientGlobalId int64
)

func generateClientId() int64 {
	return atomic.AddInt64(&ctrlerClientGlobalId, 1)
}
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	c := new(Clerk)
	c.servers = servers
	c.size = len(servers)
	c.Uid = generateClientId()
	return c
}

func (ck *Clerk) Query(num int) Config {
	req := QueryArgs{
		Idx: num,
		ClientInfo: ClientInfo{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&ck.recentLeader)

	//ck.info("开始Query %+v", req)
	for {
		for range ck.servers {
			var resp QueryResponse
			ck.servers[i].Call("ShardCtrler.Query", &req, &resp)
			if resp.RPCInfo == SUCCESS {
				atomic.SwapInt32(&ck.recentLeader, i)
				//ck.info("成功Query %+v", resp)
				return resp.Config
			}
			i = (i + 1) % int32(ck.size)
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	req := JoinArgs{
		Servers: servers,
		ClientInfo: ClientInfo{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&ck.recentLeader)

	//ck.info("开始Join %+v", req)
	//defer ck.info("成功Join %+v", req)
	for {
		for range ck.servers {
			var resp JoinReply
			ck.servers[i].Call("ShardCtrler.Join", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(ck.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Leave(gids []int) {
	req := LeaveArgs{
		GIDs: gids,
		ClientInfo: ClientInfo{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&ck.recentLeader)

	//ck.info("开始Leave %+v", req)
	//defer ck.info("成功Leave %+v", req)
	for {
		for range ck.servers {
			var resp LeaveReply
			ck.servers[i].Call("ShardCtrler.Leave", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(ck.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	req := MoveArgs{
		MoveAc: MoveAc{
			Shard: shard,
			GID:   gid,
		},
		ClientInfo: ClientInfo{
			Uid: ck.Uid,
			Seq: atomic.AddInt64(&ck.Seq, 1),
		},
	}

	i := atomic.LoadInt32(&ck.recentLeader)

	//ck.info("开始Move %+v", req)
	//defer ck.info("成功Move %+v", req)
	for {
		for range ck.servers {
			var resp MoveReply
			ck.servers[i].Call("ShardCtrler.Move", &req, &resp)
			switch resp.RPCInfo {
			case SUCCESS, DUPLICATE_REQUEST:
				atomic.SwapInt32(&ck.recentLeader, i)
				return
			default:
				i = (i + 1) % int32(ck.size)
			}
		}
		time.Sleep(CLIENT_REQUEST_INTERVAL)
	}
}
