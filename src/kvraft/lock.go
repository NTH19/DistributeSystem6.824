package kvraft

import (
	"fmt"
	"time"
)

const (
	LOCK_TIMEOUT = 1000 * time.Millisecond
)

func (kv *KVServer) lock(namespace string) {
	kv.mu.Lock()
	kv.lockName = namespace
	kv.lockTime = time.Now()
	//fmt.Println("LOCK:", namespace)
}

func (kv *KVServer) unlock() {
	if d := time.Since(kv.lockTime); d >= LOCK_TIMEOUT {
		panic(fmt.Sprintf("[KV %d] UNLOCK[%s] too long, cost %+v", kv.me, kv.lockName, d))
	}
	kv.mu.Unlock()
}
