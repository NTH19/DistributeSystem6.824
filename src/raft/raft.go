package raft

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Trigger struct {
	On        bool
	C         chan int
	StartTime int64
	Elapsed   bool
}

func NewTrigger() *Trigger {
	return &Trigger{
		StartTime: time.Now().UnixNano(),
		C:         make(chan int),
		On:        true,
	}
}

func (t *Trigger) Wait() {
	<-t.C
}

func (t *Trigger) Elapse() {
	t.Elapsed = true
	t.On = false
	close(t.C)
}

func (t *Trigger) Close() {
	t.On = false
	close(t.C)
}

// Trigger naturally elapses
func (rf *Raft) elapseTrigger(d time.Duration, st int64) {
	time.Sleep(d)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Elapse()
	}
	/*-----------------------------------------*/
}

// close the Trigger in advance
// caller is within a critical section, no need to lock
func (rf *Raft) closeTrigger(st int64) {
	if rf.trigger != nil && rf.trigger.On && rf.trigger.StartTime == st {
		rf.trigger.Close()
	}
}

// reset the Trigger
// caller is within a critical section, no need to lock
func (rf *Raft) resetTrigger() {
	if rf.trigger != nil && rf.trigger.On {
		rf.trigger.Close()
	}
}
func TimerForTest(c chan int) {
	var t int
	var s string
outer:
	for {
		select {
		case <-c:
			break outer
		default:
			t++
			s += "*"
			time.Sleep(time.Second)
		}
		fmt.Printf("%02d second %s\n", t, s)
		if t >= 100 {
			panic("panic_too_long")
		}
	}
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)
const (
	TIMER_BASE         = 200
	TIMER_RANGE        = 300
	HEARTBEAT_INTERVAL = 100 * time.Millisecond
	APPLY_INTERVAL     = 100 * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	size int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	voteFor     int
	logs        []LogEntry
	offset      int
	snapIndex   int
	snapTerm    int
	commitIndex int
	applyIndex  int
	nextIndex   []int
	matchIndex  []int
	votes       int
	applyChan   chan ApplyMsg
	appendChan  chan int
	trigger     *Trigger
	role        int
}
type RequestVoteRequest struct {
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}
type RequestVoteResponse struct {
	ResponseId   int
	ResponseTerm int
	Info         string
}

const (
	TERM_OUTDATED    = "任期过期"
	SUCCESS          = "成功请求"
	LOG_INCONSISTENT = "日志异步"
	VOTE_GRANTED     = "收到选票"
	VOTE_REJECTED    = "拒绝选票"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.role == LEADER

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.makeRaftStateBytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("bad deserialization")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.logs = logs
		rf.offset = offset
		rf.snapIndex = lastIncludedIndex
		rf.snapTerm = lastIncludedTerm
	}
}
func (rf *Raft) InstallSnapshotHandler(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {

	rf.mu.Lock()
	//rf.info("InstallSnapshot RPC receives %+v", *req)

	resp.ResponseTerm = rf.currentTerm

	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TERM_OUTDATED
		//rf.info("InstallSnapshot RPC returns")
		rf.mu.Unlock()
		return
	}

	rf.resetTrigger()
	resp.Info = SUCCESS
	if req.LeaderTerm > rf.currentTerm {
		rf.currentTerm = req.LeaderTerm
		rf.persist()
		rf.role = FOLLOWER
	}

	if rf.snapIndex >= req.SnapIndex {
		//rf.info("InstallSnapshot RPC returns")
		rf.mu.Unlock()
		return
	}

	msg := ApplyMsg{
		CommandValid: false,
		// For 2D:
		SnapshotValid: true,
		Snapshot:      req.Snapshot.Data,
		SnapshotIndex: req.SnapIndex,
		SnapshotTerm:  req.SnapTerm,
	}
	rf.mu.Unlock()
	rf.applyChan <- msg

	rf.mu.Lock()
	//rf.info("InstallSnapshot RPC returns")
	rf.mu.Unlock()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(snapTerm int, snapIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapIndex >= snapIndex {
		return false
	}

	rf.snapIndex = snapIndex
	rf.snapTerm = snapTerm
	sliIdx := rf.snapIndex - rf.offset
	rf.offset = rf.snapIndex + 1

	if sliIdx >= 0 && sliIdx < len(rf.logs)-1 {
		rf.logs = rf.logs[sliIdx+1:]
		//rf.info("快照没有覆盖所有日志")
	} else {
		rf.logs = []LogEntry{}
		//rf.info("全量快照")
	}
	s := Snapshot{
		SnapIndex: snapIndex,
		SnapTerm:  snapTerm,
		Data:      snapshot,
	}
	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)

	rf.applyIndex = rf.snapIndex
	//rf.info("Raft层快照更新完毕")
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, stateBytes []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.info("%03d快照完毕", index)
	//rf.info("%03d快照开始", index)

	sliIdx := index - rf.offset

	if sliIdx < 0 {
		//rf.info("快照Index过期，无需快照")
		return
	}

	if sliIdx >= len(rf.logs) {
		panic("非法快照Idx")
	}
	rf.snapIndex = index
	rf.snapTerm = rf.logs[sliIdx].Term
	rf.offset = index + 1

	s := Snapshot{
		SnapIndex: rf.snapIndex,
		SnapTerm:  rf.snapTerm,
		Data:      stateBytes,
	}

	rf.logs = rf.logs[sliIdx+1:]
	state, snap := rf.makeRaftStateBytes(), rf.makeSnapshotBytes(s)
	rf.persister.SaveStateAndSnapshot(state, snap)
}

func (rf *Raft) RequestVoteHandler(req *RequestVoteRequest, resp *RequestVoteResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.info("RequestVote RPC returns")
	//rf.info("RequestVote RPC receives %+v", *req)

	resp.ResponseTerm = rf.currentTerm

	if req.CandidateTerm < rf.currentTerm {
		//rf.info("reject VoteRequest from %d, my term is newer", req.CandidateId)
		resp.Info = TERM_OUTDATED
		return
	}
	var lastIndex, lastTerm int
	if len(rf.logs) != 0 {
		lastIndex, lastTerm = rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	} else {
		lastIndex, lastTerm = rf.snapIndex, rf.snapTerm
	}

	if req.CandidateTerm > rf.currentTerm {
		rf.currentTerm = req.CandidateTerm
		rf.voteFor = -1
		rf.persist()
		rf.role = FOLLOWER
	}

	if rf.voteFor != -1 && rf.voteFor != req.CandidateId {
		//rf.info("reject VoteRequest from %d, already voted for %d", req.CandidateId, rf.votedFor)
		resp.Info = VOTE_REJECTED
		return
	}
	if lastTerm > req.LastLogTerm || (lastTerm == req.LastLogTerm && lastIndex > req.LastLogIndex) {

		resp.Info = VOTE_REJECTED
		return
	}
	rf.voteFor = req.CandidateId
	rf.persist()
	resp.Info = VOTE_GRANTED
	rf.resetTrigger()
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

func (rf *Raft) sendRequestVote(server int, st int64) {

	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}

	var req RequestVoteRequest
	var resp RequestVoteResponse

	var lastIndex, lastTerm int
	if len(rf.logs) != 0 {
		lastIndex, lastTerm = rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	} else {
		lastIndex, lastTerm = rf.snapIndex, rf.snapTerm
	}

	req = RequestVoteRequest{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  lastIndex,
		LastLogTerm:   lastTerm,
	}

	rf.mu.Unlock()

	rf.peers[server].Call("Raft.RequestVoteHandler", &req, &resp)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != CANDIDATE {
		return
	}

	switch resp.Info {
	case VOTE_GRANTED:

		if rf.currentTerm != req.CandidateTerm {
			return
		}
		rf.votes++
		if rf.votes > rf.size/2 {
			if rf.role == LEADER {
				return
			}

			rf.role = LEADER
			//rf.info("========LEADER ELECTED! votes=%d, Term=%d========", rf.votes, rf.currentTerm)
			var lastLogIndex int
			if len(rf.logs) != 0 {
				lastLogIndex = rf.logs[len(rf.logs)-1].Index
			} else {
				lastLogIndex = rf.snapIndex
			}

			for i := 0; i < rf.size; i++ {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = lastLogIndex

			//rf.info("Upon election, match=%+v,next=%+v", rf.matchIndex, rf.nextIndex)
			rf.closeTrigger(st)
		}

	case TERM_OUTDATED:
		if rf.currentTerm >= resp.ResponseTerm {
			return
		}
		rf.currentTerm = resp.ResponseTerm
		rf.role = FOLLOWER
		rf.persist()
		//rf.info("term is out of date and roll back, %d<%d", rf.currentTerm, resp.ResponseTerm)
		rf.closeTrigger(st)
	case VOTE_REJECTED:
	default:
		//rf.info("VoteRequest to server %d timeout", server)
	}

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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == LEADER {
		if len(rf.logs) > 0 {
			index = rf.logs[len(rf.logs)-1].Index + 1
		} else {
			index = rf.snapIndex + 1
		}
		term = rf.currentTerm
		isLeader = true
		rf.logs = append(rf.logs, LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		})
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		//not blocking
		go func() { rf.appendChan <- 0 }()
	}

	return index, term, isLeader
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
func (rf *Raft) FLoop() {

	for !rf.killed() {
		rf.mu.Lock()

		timeout := (TIMER_BASE + time.Duration(rand.Intn(TIMER_RANGE))) * time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "reset timer=%+v, startTime=%+v", timeout, rf.trigger.StartTime)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()

		rf.trigger.Wait()

		rf.mu.Lock()
		switch {
		case rf.trigger.Elapsed:
			//rf.info("Follower turns to Candidate, timeout=%+v", timeout)
			rf.role = CANDIDATE
			rf.trigger = nil
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.votes = 1
			rf.persist()
			rf.mu.Unlock()
			return
		default:
			rf.role = FOLLOWER
			rf.trigger = nil
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CLoop() {

	for !rf.killed() {

		rf.mu.Lock()
		//rf.info("start new election")
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		timeout := TIMER_BASE*time.Millisecond + time.Duration(rand.Intn(TIMER_RANGE))*time.Millisecond
		rf.trigger = NewTrigger()
		// Debug(rf, "candidate timer=%+v, term=%d", timeout, rf.currentTerm)
		go rf.elapseTrigger(timeout, rf.trigger.StartTime)

		rf.mu.Unlock()

		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendRequestVote(i, rf.trigger.StartTime)
		}

		rf.trigger.Wait()

		rf.mu.Lock()
		rf.trigger = nil
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.votes = 1
		rf.persist()
		rf.mu.Unlock()
	}
}

type Snapshot struct {
	Data      []byte
	SnapIndex int
	SnapTerm  int
}
type InstallSnapshotRequest struct {
	LeaderTerm int
	LeaderId   int
	Snapshot
}

type InstallSnapshotResponse struct {
	ResponseId   int
	ResponseTerm int
	Info         string
}

func (rf *Raft) leaderTryUpdateCommitIndex() {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Index <= rf.commitIndex {
			break
		}
		n := rf.logs[i].Index
		if rf.logs[i].Term == rf.currentTerm {
			var replicates int

			for j := 0; j < rf.size; j++ {
				if j == rf.me {
					replicates++
					continue
				}
				if rf.matchIndex[j] >= n {
					replicates++
				}
			}
			if replicates > rf.size/2 {
				_ = rf.commitIndex
				rf.commitIndex = n
			}
		}
	}
}
func (rf *Raft) searchRightIndex(conflictTerm int) int {
	l, r := 0, len(rf.logs)-1
	for l < r {
		m := (l + r) / 2

		if rf.logs[m].Term == conflictTerm {
			l = m + 1
		} else if rf.logs[m].Term > conflictTerm {
			r = m - 1
		} else {
			l = m + 1
		}
	}
	return l
}

type AppendEntriesRequest struct {
	LeaderTerm        int
	LeaderId          int
	LeaderCommitIndex int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
}
type AppendEntriesResponse struct {
	ResponseId    int
	ResponseTerm  int
	ConflictIndex int
	ConflictTerm  int
	Info          string
}

func (rf *Raft) sendHeartBeat(server int) {

	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	pos := prevLogIndex - rf.offset

	if pos < -1 {
		//rf.info(RAFT_FORMAT+"需要快照，因为next=%+v", server, rf.nextIndex)
		var snapReq InstallSnapshotRequest
		var snapResp InstallSnapshotResponse

		snapReq.LeaderId = rf.me
		snapReq.LeaderTerm = rf.currentTerm
		snapReq.Snapshot = rf.lastestSnapshot()
		rf.mu.Unlock()

		rf.peers[server].Call("Raft.InstallSnapshotHandler", &snapReq, &snapResp)

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}

		switch snapResp.Info {
		case SUCCESS:
			rf.nextIndex[server] = rf.snapIndex + 1
			//rf.info("快照RPC->"+RAFT_FORMAT+"成功，更新next=%+v", server, rf.nextIndex)
			rf.mu.Unlock()
			return

		case TERM_OUTDATED:
			rf.role = FOLLOWER
			rf.currentTerm = snapResp.ResponseTerm
			//rf.info("快照RPC->"+RAFT_FORMAT+"返回TermOutdated，更新Term %d->%d", server, rf.currentTerm, snapResp.ResponseTerm)
			rf.persist()
			rf.mu.Unlock()
			return
		default:
			//rf.info("快照RPC->"+RAFT_FORMAT+"网络波动", server)
			rf.mu.Unlock()
			return
		}
	}

	var req AppendEntriesRequest
	var resp AppendEntriesResponse

	if pos == -1 {
		req.PrevLogTerm = rf.snapTerm
	} else {
		req.PrevLogTerm = rf.logs[pos].Term
	}
	req.PrevLogIndex = prevLogIndex
	req.Entries = rf.logs[pos+1:]
	req.LeaderTerm = rf.currentTerm
	req.LeaderId = rf.me
	req.LeaderCommitIndex = rf.commitIndex

	//rf.info("心跳RPC->"+RAFT_FORMAT+"%+v", server, req)
	rf.mu.Unlock()
	rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}

	switch resp.Info {
	case SUCCESS:
		if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
			rf.matchIndex[server] = req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		//rf.info("心跳RPC->"+RAFT_FORMAT+"成功,更新match=%+v,next=%+v", server, rf.matchIndex, rf.nextIndex)
		rf.leaderTryUpdateCommitIndex()

	case TERM_OUTDATED:
		rf.currentTerm = resp.ResponseTerm
		rf.role = FOLLOWER
		//rf.info("心跳RPC->"+RAFT_FORMAT+"返回TermOutdated, 更新Term %d->%d", server, rf.currentTerm, resp.ResponseTerm)
		rf.persist()
	case LOG_INCONSISTENT:
		if resp.ConflictIndex == 0 {
			rf.nextIndex[server] = 1
		}
		idx := rf.searchRightIndex(resp.ConflictTerm)
		if len(rf.logs) == 0 {
			rf.nextIndex[server] = resp.ConflictIndex
		} else if rf.logs[idx].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[idx].Index + 1
		} else if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
			rf.nextIndex[server] = rf.logs[idx-1].Index + 1
		} else {
			rf.nextIndex[server] = resp.ConflictIndex
		}
		//rf.info("心跳RPC与"+RAFT_FORMAT+"日志不一致, ConflictIdx=%d, ConflictTerm=%d, 更新next=%+v", server, resp.ConflictIndex, resp.ConflictTerm, rf.nextIndex)

	default:
		//rf.info("追加RPC->"+RAFT_FORMAT+"网络波动", server)
	}
	/*-----------------------------------------*/
}
func (rf *Raft) LLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < rf.size; i++ {
			if i == rf.me {
				continue
			}

			go rf.sendHeartBeat(i)
		}
		sleeper := time.NewTimer(HEARTBEAT_INTERVAL)
		<-sleeper.C
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		r := rf.role
		rf.mu.Unlock()
		switch r {
		case FOLLOWER:
			rf.FLoop()
		case CANDIDATE:
			rf.CLoop()
		case LEADER:
			rf.LLoop()
		}

	}
}
func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
func (rf *Raft) receiverTryUpdateCommitIndex(req *AppendEntriesRequest) {
	if req.LeaderCommitIndex > rf.commitIndex {
		_ = rf.commitIndex
		if len(rf.logs) > 0 {
			rf.commitIndex = min(req.LeaderCommitIndex, rf.logs[len(rf.logs)-1].Index)
		}
	}
}
func (rf *Raft) AppendEntriesHandler(req *AppendEntriesRequest, resp *AppendEntriesResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.info("AppendEntries RPC returns")

	//rf.info("AppendEntries RPC receives %+v", *req)
	resp.ResponseTerm = rf.currentTerm

	// 1. reply false if term < currentTerm (§5.1)
	if req.LeaderTerm < rf.currentTerm {
		resp.Info = TERM_OUTDATED
		return
	}
	rf.resetTrigger()

	if req.LeaderTerm > rf.currentTerm {
		rf.currentTerm = req.LeaderTerm
		rf.persist()
		rf.role = FOLLOWER
	}
	sliceIdx := req.PrevLogIndex - rf.offset

	switch {

	case sliceIdx >= len(rf.logs):
		resp.Info = LOG_INCONSISTENT
		resp.ConflictIndex = len(rf.logs) + rf.offset - 1
		resp.ConflictTerm = -1
		return

	case sliceIdx == -1 && req.PrevLogIndex == 0:

	case sliceIdx == -1 && req.PrevLogIndex == rf.snapIndex:

	case sliceIdx < 0:
		resp.Info = LOG_INCONSISTENT
		resp.ConflictIndex = 0
		resp.ConflictTerm = -1

		return

	default:
		if rf.logs[sliceIdx].Term != req.PrevLogTerm {
			resp.ConflictTerm = rf.logs[sliceIdx].Term
			for i := 0; i <= sliceIdx; i++ {
				if rf.logs[i].Term == resp.ConflictTerm {
					resp.ConflictIndex = rf.logs[i].Index
					break
				}
			}

			resp.Info = LOG_INCONSISTENT
			return
		}
	}

	resp.Info = SUCCESS

	i := sliceIdx + 1
	j := 0

	es := make([]LogEntry, len(req.Entries))
	copy(es, req.Entries)
	for j < len(es) {
		if i == len(rf.logs) {
			rf.logs = append(rf.logs, es[j])
		} else if rf.logs[i].Term != es[j].Term {
			rf.logs = rf.logs[:i]
			rf.logs = append(rf.logs, es[j])
		}
		i++
		j++
	}
	rf.persist()

	rf.receiverTryUpdateCommitIndex(req)
}
func (rf *Raft) sendAppendEntries(server int) {
outer:
	for !rf.killed() {

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Index < rf.nextIndex[server] {
			rf.mu.Unlock()
			return
		}
		if len(rf.logs) == 0 && rf.snapIndex < rf.nextIndex[server] {
			rf.mu.Unlock()
			return
		}

		prevLogIndex := rf.nextIndex[server] - 1
		pos := prevLogIndex - rf.offset

		if (len(rf.logs) == 0) || (pos < -1) {
			//rf.info(RAFT_FORMAT+"需要快照，因为next=%+v", server, rf.nextIndex)
			var snapReq InstallSnapshotRequest
			var snapResp InstallSnapshotResponse
			snapReq.LeaderId = rf.me
			snapReq.LeaderTerm = rf.currentTerm
			snapReq.Snapshot = rf.lastestSnapshot()
			rf.mu.Unlock()

			rf.peers[server].Call("Raft.InstallSnapshotHandler", &snapReq, &snapResp)

			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			switch snapResp.Info {
			case SUCCESS:
				rf.nextIndex[server] = rf.snapIndex + 1
				//rf.info("快照RPC->"+RAFT_FORMAT+"成功，更新next=%+v", server, rf.nextIndex)
				rf.mu.Unlock()
				continue outer
			case TERM_OUTDATED:
				rf.role = FOLLOWER
				rf.currentTerm = snapResp.ResponseTerm
				//rf.info("快照RPC->"+RAFT_FORMAT+"返回TermOutdated，更新Term %d->%d", server, rf.currentTerm, snapResp.ResponseTerm)
				rf.persist()
				rf.mu.Unlock()
				return
			default:
				//rf.info("快照RPC->"+RAFT_FORMAT+"网络波动", server)
				rf.mu.Unlock()
				continue outer
			}
		}

		var req AppendEntriesRequest
		var resp AppendEntriesResponse

		if pos == -1 {
			req.PrevLogTerm = rf.snapTerm
		} else {
			req.PrevLogTerm = rf.logs[pos].Term
		}
		req.PrevLogIndex = prevLogIndex
		req.Entries = make([]LogEntry, len(rf.logs[pos+1:]))
		copy(req.Entries, rf.logs[pos+1:])
		req.LeaderTerm = rf.currentTerm
		req.LeaderId = rf.me
		req.LeaderCommitIndex = rf.commitIndex

		//rf.info("追加RPC->"+RAFT_FORMAT+"%+v", server, req)
		rf.mu.Unlock()
		rf.peers[server].Call("Raft.AppendEntriesHandler", &req, &resp)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}

		switch resp.Info {

		case SUCCESS:
			if n := req.PrevLogIndex + len(req.Entries); n > rf.matchIndex[server] {
				rf.matchIndex[server] = n
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			//rf.info("追加RPC->"+RAFT_FORMAT+"成功,更新match=%+v,next=%+v", server, rf.matchIndex, rf.nextIndex)
			rf.leaderTryUpdateCommitIndex()
			rf.mu.Unlock()
			return
		case TERM_OUTDATED:
			rf.role = FOLLOWER
			rf.currentTerm = resp.ResponseTerm
			//rf.info("追加RPC->"+RAFT_FORMAT+"返回TermOutdated, 更新Term %d->%d", server, rf.currentTerm, resp.ResponseTerm)
			rf.persist()
			rf.mu.Unlock()
			return
		case LOG_INCONSISTENT:
			if resp.ConflictIndex == 0 {
				rf.nextIndex[server] = 1
			}

			idx := rf.searchRightIndex(resp.ConflictTerm)
			if len(rf.logs) == 0 {
				rf.nextIndex[server] = resp.ConflictIndex
			} else if rf.logs[idx].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx].Index + 1
			} else if idx > 0 && rf.logs[idx-1].Term == resp.ConflictTerm {
				rf.nextIndex[server] = rf.logs[idx-1].Index + 1
			} else {
				rf.nextIndex[server] = resp.ConflictIndex
			}
			//rf.info("追加RPC与"+RAFT_FORMAT+"日志不一致, ConflictIdx=%d, ConflictTerm=%d, 更新next=%+v", server, resp.ConflictIndex, resp.ConflictTerm, rf.nextIndex)
			rf.mu.Unlock()
		default:
			//rf.info("追加RPC->"+RAFT_FORMAT+"网络波动", server)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) replicateLoop() {
	for !rf.killed() {
		<-rf.appendChan
		for i := 0; i < rf.size; i++ {
			if rf.me == i {
				continue
			}

			go rf.sendAppendEntries(i)
		}
	}
}

func (rf *Raft) batchApply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.applyIndex {
			rf.applyIndex++
			idx := rf.applyIndex - rf.offset
			if idx < 0 {
				rf.mu.Unlock()
				continue
			}
			entry := rf.logs[idx]
			msg := ApplyMsg{
				CommandValid: entry.Command != nil,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			//rf.info("应用日志 [%d|%d]", msg.CommandIndex, msg.CommandTerm)
			rf.mu.Unlock()
			rf.applyChan <- msg

		} else {
			rf.mu.Unlock()
			return
		}
	}
}
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.batchApply()
		time.Sleep(APPLY_INTERVAL)
	}
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
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.size = len(peers)
	rf.applyChan = applyCh
	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.appendChan = make(chan int)
	rf.offset = 1

	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	go rf.ticker()
	go rf.replicateLoop()
	go rf.applyLoop()

	//Debug("========"+RAFT_FORMAT+"STARTED========", rf.me)
	return rf
}
