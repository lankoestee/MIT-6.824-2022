package raft

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

// <<<<<<<<<< Log 日志及日志获取方法 <<<<<<<<<<

type LogEntry struct {
	Term    int         // 任期
	Command interface{} // 命令
}

type LogEntries []LogEntry // 多日志条目

// 获取指定索引的日志条目
func (logEntries LogEntries) getEntry(index int) *LogEntry {
	if index < 0 {
		log.Panic("LogEntries.getEntry: index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}
	if index > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[index-1]
}

// 获取最后一条日志的索引和任期
func (logEntries LogEntries) lastLogInfo() (index, term int) {
	index = len(logEntries)
	logEntry := logEntries.getEntry(index)
	return index, logEntry.Term
}

// 获取指定范围的日志条目
func (logEntries LogEntries) getSlice(startIndex, endIndex int) LogEntries {
	if startIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: startIndex out of range. \n")
	}
	if endIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: endIndex out of range.\n")
	}
	if startIndex > endIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panic("LogEntries.getSlice: startIndex > endIndex.\n")
	}
	return logEntries[startIndex-1 : endIndex-1]
}

// >>>>>>>>>> Log 日志及日志获取方法 >>>>>>>>>>

// <<<<<<<<<< Time 超时函数的实现 <<<<<<<<<<

// 均以毫秒为单位
const BaseHeartbeatTimeout int64 = 300 // 基准心跳超时时间
const BaseElectionTimeout int64 = 1000 // 基准选举超时时间
const HeartbeatInterval int64 = 100    // 心跳间隔

const RandomFactor float64 = 0.8 // 随机因子

// 随机产生选举超时时间
func randElectionTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseElectionTimeout) * RandomFactor)
	return time.Duration(extraTime+BaseElectionTimeout) * time.Millisecond
}

// 随机产生心跳超时时间
func randHeartbeatTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseHeartbeatTimeout) * RandomFactor)
	return time.Duration(extraTime+BaseHeartbeatTimeout) * time.Millisecond
}

// 设置选举超时时间
func (rf *Raft) setElectionTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.electionTime = t
}

// 设置心跳超时时间
func (rf *Raft) setHeartbeatTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.heartbeatTime = t
}

// >>>>>>>>>> Time 超时函数的实现 >>>>>>>>>>

// <<<<<<<<<< Raft 主要结构以及状态机的实现 <<<<<<<<<<

type RaftState int // 节点状态

const (
	Follower  RaftState = iota // 跟随者
	Candidate                  // 候选人
	Leader                     // 领导者
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有服务器上的持久状态
	currentTerm int        // 服务器任务的最新任期，单调递增
	votedFor    int        // 为哪个节点投票，如果没有投票则为null(-1)
	log         LogEntries // 日志条目

	// 所有服务器上的易失状态
	commitIndex int // 表示日志条目中已提交条目的最高下标
	lastApplied int // 表示应用于状态机的最高日志条目下标

	// 领导者上的易失状态
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	// 本地状态
	leaderId      int       // 领导者的ID
	state         RaftState // 节点状态
	heartbeatTime time.Time // 心跳超时时间
	electionTime  time.Time // 选举超时时间
}

// 向状态机传输日志条目数据结构
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 获取当前的任期与状态
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
const TickInterval int64 = 30

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 如果是领导者，发送心跳
		if rf.state == Leader && time.Now().After(rf.heartbeatTime) {
			Debug(dTimer, "S%d: Leader timeout. Start heartbeat.", rf.me)
			rf.sendEntries(true) // 发送心跳
		}
		// 如果是跟随者，开始选举
		if time.Now().After(rf.electionTime) {
			Debug(dTimer, "S%d: Election timeout. Start election.", rf.me)
			rf.raiseElection() // 开始选举
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.setElectionTimeout(randHeartbeatTimeout())
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
	Debug(dClient, "S%d Started at T%d. LLI: %d, LLT: %d.", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// >>>>>>>>>> Raft 主要结构以及状态机的实现 >>>>>>>>>>

// <<<<<<<<<< 2A 选举部分实现 <<<<<<<<<<

// RequestVote RPC 请求投票信息，论文图2所示
type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 请求选票的候选人的ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// RequestVote RPC reply 请求投票信息的回复
type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// 检查是否需要更新任期以及转变为Follower状态
func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		Debug(dTerm, "S%d Term is higher, updating term to T%d, setting state to follower. (%d > %d)",
			rf.me, term, term, rf.currentTerm)
		rf.state = Follower   // 转变为跟随者
		rf.currentTerm = term // 更新任期
		rf.votedFor = -1      // 重置投票
		return true
	}
	return false
}

// 投票实现核心函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d <- S%d Received vote request at T%d.", rf.me, args.CandidateId, rf.currentTerm)
	reply.VoteGranted = false // 默认拒绝投票

	// 如果candidate的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Term is lower, rejecting the vote. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	// 如果candidate的任期大于当前任期，更新任期
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	// 如果没有投票或者已经投票给了candidate，且candidate的日志至少和自己一样新，投票给candidate
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			Debug(dVote, "S%d Granting vote to S%d at T%d.", rf.me, args.CandidateId, args.Term)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			Debug(dTimer, "S%d Resetting ELT, wait for next potential election timeout.", rf.me)
			rf.setElectionTimeout(randElectionTimeout())
		} else {
			Debug(dVote, "S%d Candidate's log not up-to-date, rejecting the vote. LLI: %d, %d. LLT: %d, %d.",
				rf.me, lastLogIndex, args.LastLogIndex, lastLogTerm, args.LastLogTerm)
		}
	} else {
		Debug(dVote, "S%d Already voted for S%d, rejecting the vote.", rf.me, rf.votedFor)
	}
}

// 发起选举
func (rf *Raft) raiseElection() {
	rf.state = Candidate
	rf.currentTerm++
	Debug(dTerm, "S%d Starting a new term. Now at T%d.", rf.me, rf.currentTerm)

	// 为自己投票
	rf.votedFor = rf.me

	// 重置选举超时时间
	rf.setElectionTimeout(randElectionTimeout())
	Debug(dTimer, "S%d Resetting ELT because of election, wait for next potential election timeout.", rf.me)
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()

	// 记录请求投票信息
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 记录目前投票结果
	voteCount := 1
	var once sync.Once

	// 并发向其他节点发送请求投票信息
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		Debug(dVote, "S%d -> S%d Sending request vote at T%d.", rf.me, peer, rf.currentTerm)
		go rf.candidateRequestVote(&voteCount, args, &once, peer)
	}
}

// 发送请求投票信息
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 处理请求投票信息
func (rf *Raft) candidateRequestVote(voteCount *int, args *RequestVoteArgs, once *sync.Once, server int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)

	// 如果请求投票信息发送成功
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%d <- S%d Received request vote reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dVote, "S%d Term is lower, invalid vote reply. (%d < %d)", rf.me, reply.Term, rf.currentTerm)
			return
		}

		// 如果请求投票信息的任期大于当前任期，更新任期
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the vote request, vote reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}

		// 检查收到的投票任期是否大于当前任期，如果是，转变为跟随者
		rf.checkTerm(reply.Term)

		// 如果收到投票
		if reply.VoteGranted {
			*voteCount++
			Debug(dVote, "S%d <- S%d Get a yes vote at T%d.", rf.me, server, rf.currentTerm)

			// 如果收到多数节点的投票：成为领导者
			if *voteCount > len(rf.peers)/2 {
				once.Do(func() {
					Debug(dLeader, "S%d Received majority votes at T%d. Become leader.", rf.me, rf.currentTerm)
					rf.state = Leader
					lastLogIndex, _ := rf.log.lastLogInfo()
					for peer := range rf.peers {
						rf.nextIndex[peer] = lastLogIndex + 1
						rf.matchIndex[peer] = 0
					}

					// 成为领导者，向其他节点发送AppendEntries RPC，宣布胜选
					rf.sendEntries(true)
				})
			}
		} else { // 如果收到拒绝投票：转变为跟随者
			Debug(dVote, "S%d <- S%d Get a no vote at T%d.", rf.me, server, rf.currentTerm)
			rf.state = Follower
		}
	}
}

// >>>>>>>>>> 2A 选举部分实现 >>>>>>>>>>

// <<<<<<<<<< 2B 日志同步部分（仅作补充，尚未实现） <<<<<<<<<<

// AppendEntries RPC 论文图2所示
type AppendEntriesArgs struct {
	Term         int        // 领导者的任期
	LeaderId     int        // 领导者的ID，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空）
	LeaderCommit int        // 领导者的已知已提交的最高日志条目的索引值
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// 处理心跳或者日志条目操作
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果是心跳，记录心跳超时时间
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	reply.Success = false

	// 如果任期小于当前任期，拒绝请求
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)",
			rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	// 如果是候选人，且任期相同，转变为跟随者
	if rf.state == Candidate && rf.currentTerm == args.Term {
		rf.state = Follower
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())

	// 如果日志不包含在prevLogIndex处的任期与prevLogTerm不匹配，拒绝请求
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.log.getEntry(args.PrevLogIndex).Term {
		Debug(dLog2, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		return
	}

	// 如果已经存在的日志条目和新的冲突（索引相同但是任期不同），删除已经存在的日志条目和它之后的所有日志条目
	for i, entry := range args.Entries {
		if rf.log.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.log.getSlice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)
	reply.Success = true
}

// 向其他节点发送心跳
func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())
	lastLogIndex, _ := rf.log.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			entries := make([]LogEntry, lastLogIndex-nextIndex+1)
			copy(entries, rf.log.getSlice(nextIndex, lastLogIndex+1))
			args.Entries = entries
			Debug(dLog, "S%d -> S%d Sending append entries at T%d. PLI: %d, PLT: %d, LC: %d. Entries: %v.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex,
				args.PrevLogTerm, args.LeaderCommit, args.Entries,
			)
			go rf.leaderSendEntries(args, peer)
		} else if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d -> S%d Sending heartbeat at T%d. PLI: %d, PLT: %d, LC: %d.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendEntries(args, peer)
		}
	}
}

// 发送心跳或者日志条目操作
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向其他节点发送心跳或者日志条目
func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		if rf.checkTerm(reply.Term) {
			return
		}
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		if reply.Success {
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			lastLogIndex, _ := rf.log.lastLogInfo()
			nextIndex := rf.nextIndex[server]
			if lastLogIndex >= nextIndex {
				Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
				entries := make([]LogEntry, lastLogIndex-nextIndex+1)
				copy(entries, rf.log.getSlice(nextIndex, lastLogIndex+1))
				newArg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.log.getEntry(nextIndex - 1).Term,
					Entries:      entries,
				}
				go rf.leaderSendEntries(newArg, server)
			}
		}
	}
}

// >>>>>>>>>> 2B 日志同步部分（仅作补充，尚未实现） >>>>>>>>>>

// <<<<<<<<<< 2C 持久化部分实现（暂未实现） <<<<<<<<<<

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// >>>>>>>>>> 2C 持久化部分实现（暂未实现） >>>>>>>>>>

// <<<<<<<<<< 2D 日志压缩及快照部分（暂未实现） <<<<<<<<<<

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// >>>>>>>>>> 2D 日志压缩及快照部分（暂未实现） >>>>>>>>>>
