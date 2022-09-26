package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"sync"

	"6.824/labgob"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (rf *Raft) ShouldSnapshot(maxSize int) bool {
	return rf.persister.RaftStateSize() > maxSize
}

func (rf *Raft) lastestSnapshot() Snapshot {
	data := rf.persister.ReadSnapshot()
	if len(data) == 0 {
		return Snapshot{}
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var s Snapshot
	if err := d.Decode(&s); err != nil {
		panic("LOG error")
	}
	//rf.info("读取最新快照 %+v", s)
	return s
}

func (rf *Raft) LastestSnapshot() Snapshot {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastestSnapshot()
}
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}
func (rf *Raft) makeSnapshotBytes(s Snapshot) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(s)
	return w.Bytes()
}
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}
func (rf *Raft) makeRaftStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)
	e.Encode(rf.snapIndex)
	e.Encode(rf.snapTerm)
	return w.Bytes()
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
