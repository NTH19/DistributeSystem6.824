package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type NodeState int32

const (
	Leader NodeState = iota
	Follower
	Candidate
)
const NONE = -1

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	state          NodeState

	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	for _, ent := range rf.logs {
		e.Encode(ent)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var votedFor int
	if d.Decode(&votedFor) != nil || d.Decode(&term) != nil {
		panic("Decode the persist Error!!!")
	}
	rf.votedFor = votedFor
	rf.currentTerm = term
	for {
		var ent Entry
		if d.Decode(&ent) != nil {
			break
		}
		rf.logs = append(rf.logs, ent)
	}
}
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state)
	return w.Bytes()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)
	} else {
		rf.logs = rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.logs[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = rf.logs[index-snapshotIndex:]
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateId  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//DPrintf("Node : %v vote For %v", rf.me, request.CandidateId)
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != NONE && rf.votedFor != request.CandidateId) {
		if request.Term > rf.currentTerm {
			rf.currentTerm = request.Term
		}
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, NONE
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = request.CandidateId
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	response.Term, response.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	ent := rf.getLastLog()
	if ent.Term < term || (ent.Term == term && ent.Index <= index) {
		return true
	}
	return false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}
type AppendEntriesReply struct {
	HasSuccess bool
	Term       int
	QuickIndex int
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.HasSuccess = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term //update its own term
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.ChangeState(Follower)
	if args.PrevLogIndex > rf.getLastLog().Index || args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.HasSuccess = rf.currentTerm, false
		return
	}
	if args.PrevLogIndex <= rf.getLastLog().Index && args.PrevLogTerm != rf.logs[args.PrevLogIndex-rf.getFirstLog().Index].Term {
		reply.Term, reply.HasSuccess = rf.currentTerm, false
		return
	}
	if len(args.Entries) != 0 {
		DPrintf("NODE %v append entries len:%v  ,prev Index%v", rf.me, len(args.Entries), args.PrevLogIndex)
		DPrintf("NODE %v ,I have Entry %v", rf.me, rf.getLastLog().Index)
	}
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			DPrintf("append batch entires %v", len(args.Entries))
			rf.logs = append(rf.logs[:entry.Index-firstIndex], args.Entries[index:]...)
			break
		}
	}
	if len(args.Entries) != 0 {
		DPrintf("After that I have Entry %v", rf.getLastLog().Index)
	}

	rf.advanceCommitIndexForFollower(args.LeaderCommit)

	reply.HasSuccess = true
	reply.Term = rf.currentTerm

}
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	if rf.commitIndex != Max(leaderCommit, rf.commitIndex) {
		rf.commitIndex = leaderCommit
		rf.applyCond.Signal()
	}
}

type InstallSnapshotResponse struct {
	Term int
}
type InstallSnapshotRequest struct {
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
	Term              int
}

func (rf *Raft) handleAppendEntriesResponse(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("Node %vhandle Append Entries %+v", rf.me, reply)
	if reply.HasSuccess {
		if len(args.Entries) != 0 {
			rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			to := make([]int, len(rf.matchIndex))
			copy(to, rf.matchIndex)
			sort.Slice(to, func(i, j int) bool {
				return i < j
			})
			if len(to)%2 == 0 {
				if rf.commitIndex != to[len(to)/2-1] {
					if rf.logs[to[len(to)/2-1]-rf.getFirstLog().Index].Term == rf.currentTerm {
						rf.commitIndex = to[len(to)/2-1]
						DPrintf("Node %v Handle Append %v  Agreement%v", rf.me, rf.commitIndex, to[len(to)/2-1])
						rf.applyCond.Signal()
					}
				}
			} else {
				if rf.commitIndex != to[len(to)/2] {
					if rf.logs[to[len(to)/2]-rf.getFirstLog().Index].Term == rf.currentTerm {
						rf.commitIndex = to[len(to)/2]
						DPrintf("Handle Append %v  Agreement%v", rf.commitIndex, to[len(to)/2])
						rf.applyCond.Signal()
					}
				}
			}
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ChangeState(Follower)
	} else {
		rf.nextIndex[peer] -= 1
		rf.matchIndex[peer] -= 1
	}
}
func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	return &InstallSnapshotRequest{}
}
func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	return true
}
func (rf *Raft) handleInstallSnapshotResponse(peer int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {

}
func (rf *Raft) genAppendEntriesRequest(peer int) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1-rf.getFirstLog().Index].Term,
		Entries:      rf.logs[rf.nextIndex[peer]-rf.getFirstLog().Index:],
	}
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// outdated snapshot
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.matchIndex[peer]
	if prevLogIndex < rf.getFirstLog().Index {
		// only snapshot can catch up
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		rf.mu.RUnlock()
		for {
			rf.mu.RLock()
			if rf.state != Leader {
				rf.mu.RUnlock()
				return
			}
			request := rf.genAppendEntriesRequest(peer)
			rf.mu.RUnlock()
			response := new(AppendEntriesReply)
			if rf.sendAppendEntries(peer, request, response) {
				rf.mu.Lock()
				rf.handleAppendEntriesResponse(peer, request, response)
				rf.mu.Unlock()
				if response.HasSuccess {
					return
				}
			}
		}
	}
}
func (rf *Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[id].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	entry := rf.appendLogEntry(command)
	rf.BroadcastHeartbeat(false)
	DPrintf("Node %v get Command %v", rf.me, entry.Index)
	index = entry.Index
	term = entry.Term
	return index, term, true
}
func (rf *Raft) appendLogEntry(command interface{}) Entry {
	ent := Entry{
		Index:   rf.getLastLog().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, ent)
	rf.nextIndex[rf.me] = rf.getLastLog().Index + 1
	rf.matchIndex[rf.me] = rf.getLastLog().Index
	return ent
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) ChangeState(state NodeState) {
	if state == Leader {
		for val, _ := range rf.peers {
			rf.matchIndex[val] = 0
			rf.nextIndex[val] = rf.getLastLog().Index + 1
		}
		rf.matchIndex[rf.me] = rf.getLastLog().Index
	}
	rf.state = state
}
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.ChangeState(Candidate)
				rf.currentTerm += 1
				DPrintf("Start Election")
				rf.StartElection()
			}
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	ent := rf.getLastLog()
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: ent.Index,
		LastLogTerm:  ent.Term,
	}
}

func (rf *Raft) StartElection() {
	request := rf.genRequestVoteRequest()
	DPrintf("{Node %v} starts election with RequestVoteRequest %+v", rf.me, request)
	// use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}
func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	if isHeartBeat {
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(150) * time.Millisecond
}
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(1000)+1500) * time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		lastApplied:    -1,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          Follower,
		currentTerm:    0,
		votedFor:       NONE,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}
