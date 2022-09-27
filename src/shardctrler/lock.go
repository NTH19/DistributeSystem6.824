package shardctrler

import (
	"fmt"
	"time"
)

func (sc *ShardCtrler) lock(lockname string) {
	sc.mu.Lock()
	sc.lockname = lockname
	sc.locktime = time.Now()
}

func (sc *ShardCtrler) unlock() {
	if d := time.Since(sc.locktime); d >= LOCK_TIMEOUT {
		fmt.Println("lock: ", sc.lockname, "too long, time: ", d)
	}
	sc.mu.Unlock()
}
