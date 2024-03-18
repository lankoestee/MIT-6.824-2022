package raft

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// <<<<<<<<<< Log 日志及日志获取方法 <<<<<<<<<<

type LogEntries []LogEntry // 多日志条目

type LogEntry struct {
	Term    int         // 任期
	Command interface{} // 命令
}

// 获取指定索引的日志条目
func (rf *Raft) getEntry(index int) *LogEntry {
	logEntries := rf.log
	logIndex := index - rf.lastIncludedIndex
	if logIndex < 0 {
		log.Panicf("LogEntries.getEntry: index too small. (%d < %d)", index, rf.lastIncludedIndex)
	}
	if logIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}
	if logIndex > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[logIndex-1]
}

// 获取最后一条日志的索引和任期
func (rf *Raft) lastLogInfo() (index, term int) {
	logEntries := rf.log
	index = len(logEntries) + rf.lastIncludedIndex
	logEntry := rf.getEntry(index)
	return index, logEntry.Term
}

// 获取指定范围的日志条目
func (rf *Raft) getSlice(startIndex, endIndex int) []LogEntry {
	logEntries := rf.log
	logStartIndex := startIndex - rf.lastIncludedIndex
	logEndIndex := endIndex - rf.lastIncludedIndex
	if logStartIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: startIndex out of range. (%d < %d)", startIndex, rf.lastIncludedIndex)
	}
	if logEndIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: endIndex out of range. (%d > %d)", endIndex, len(logEntries)+1+rf.lastIncludedIndex)
	}
	if logStartIndex > logEndIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panicf("LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
	}
	return append([]LogEntry(nil), logEntries[logStartIndex-1:logEndIndex-1]...)
}

// 返回指定任期日志的第一和最后一个索引
func (rf *Raft) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	logEntries := rf.log
	if term == 0 {
		return 0, 0
	}
	minIndex, maxIndex = math.MaxInt, -1
	for i := rf.lastIncludedIndex + 1; i <= rf.lastIncludedIndex+len(logEntries); i++ {
		if rf.getEntry(i).Term == term {
			minIndex = min(minIndex, i)
			maxIndex = max(maxIndex, i)
		}
	}
	if maxIndex == -1 {
		return -1, -1
	}
	return
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

// Raft结构体，主要根据论文图2而来
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
	commitIndex int           // 表示日志条目中已提交条目的最高下标
	lastApplied int           // 表示应用于状态机的最高日志条目下标
	applyCh     chan ApplyMsg // 应用日志的通道

	// 领导者上的易失状态
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	// 本地状态
	leaderId      int       // 领导者的ID
	state         RaftState // 节点状态
	heartbeatTime time.Time // 心跳超时时间
	electionTime  time.Time // 选举超时时间

	// 应用于快照的状态
	lastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	lastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	snapshot          []byte // 内存中的快照

	// 将服务快照提供给应用线程的临时位置
	waitingIndex    int    // 等待应用的索引
	waitingTerm     int    // 等待应用的任期
	waitingSnapshot []byte // 等待应用的快照
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// 定期将已提交日志应用到状态机
func (rf *Raft) applyLogsLoop() {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed messages to apply, and apply them only after unlocked
		var appliedMsgs []ApplyMsg

		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

		if rf.waitingSnapshot != nil {
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingTerm,
				SnapshotIndex: rf.waitingIndex,
			})
			rf.waitingSnapshot = nil
		} else {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.getEntry(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				})
				Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			rf.applyCh <- msg
		}
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

// 启动Raft实例的命令提交过程
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log) + rf.lastIncludedIndex
	term = rf.currentTerm
	Debug(dLog, "S%d Add command at T%d. LI: %d, Command: %v\n", rf.me, term, index, command)
	rf.persist()
	rf.sendEntries(false)

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker函数是Raft服务器的定时器，用于发送心跳或开始选举。
const TickInterval int64 = 30

func (rf *Raft) ticker() {
	for !rf.killed() {
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

// Make函数用于创建一个Raft服务器，其中的peers数组包含了所有的服务器
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.setElectionTimeout(randHeartbeatTimeout())
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = 1
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	Debug(dClient, "S%d Started at T%d. LLI: %d, LLT: %d.", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)

	// start ticker goroutine to start elections
	go rf.ticker()

	// Apply logs periodically until the last committed index to make sure state machine is up-to-date.
	go rf.applyLogsLoop()

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
		rf.leaderId = -1      // 重置领导者
		rf.persist()          // 持久化
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
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			Debug(dVote, "S%d Granting vote to S%d at T%d.", rf.me, args.CandidateId, args.Term)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
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

	// 自加自身任期
	rf.currentTerm++
	Debug(dTerm, "S%d Starting a new term. Now at T%d.", rf.me, rf.currentTerm)

	// 为自己投票
	rf.votedFor = rf.me
	rf.persist()

	// 重置选举超时时间
	rf.setElectionTimeout(randElectionTimeout())
	Debug(dTimer, "S%d Resetting ELT because of election, wait for next potential election timeout.", rf.me)
	lastLogIndex, lastLogTerm := rf.lastLogInfo()

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
					lastLogIndex, _ := rf.lastLogInfo()
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

// <<<<<<<<<< 2B 日志同步部分实现 <<<<<<<<<<

// AppendEntries RPC 论文图2所示
type AppendEntriesArgs struct {
	Term         int        // 领导者的任期
	LeaderId     int        // 领导者的ID，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空）
	LeaderCommit int        // 领导者的已知已提交的最高日志条目的索引值
}

// AppendEntries RPC 回复的数据结构
type AppendEntriesReply struct {
	Term    int  // 当前任期，用于领导者更新自己
	XTerm   int  // 冲突日志条目的任期（如果有的话）
	XIndex  int  // 第一个具有该任期的日志条目的索引（如果有的话）
	XLen    int  // 日志的长度
	Success bool // 如果跟随者包含与PrevLogIndex和PrevLogTerm匹配的日志条目，则为true
}

// AppendEntries Follower接收Leader的追加/心跳包
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查是否收到了心跳包
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}

	reply.Success = false

	// 如果收到的Term小于当前Term，拒绝追加日志的请求
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)",
			rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	// 如果当前节点处于Candidate状态并且收到的Term与当前Term相同，将状态转换为Follower
	if rf.state == Candidate && rf.currentTerm == args.Term {
		rf.state = Follower
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
	}

	// 检查当前节点的Term，并将其设置为reply的Term
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	// 重置选举超时时间，并随机生成下一次选举超时时间
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())

	// 设置当前节点的leaderId为收到的Leader的ID
	rf.leaderId = args.LeaderId

	// 如果PrevLogIndex小于当前节点的lastIncludedIndex，表示之前的日志已经被快照截断了
	if args.PrevLogIndex < rf.lastIncludedIndex {
		alreadySnapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		// 如果已经快照截断的日志长度小于等于收到的Entries的长度，说明日志部分匹配，需要将截断的部分补充回来
		if alreadySnapshotLogLen <= len(args.Entries) {
			newArgs := &AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				PrevLogTerm:  rf.lastIncludedTerm,
				PrevLogIndex: rf.lastIncludedIndex,
				Entries:      args.Entries[alreadySnapshotLogLen:],
				LeaderCommit: args.LeaderCommit,
			}
			args = newArgs
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, readjusting. PLI: %d, PLT:%d, Entries: %v.",
				rf.me, args.PrevLogIndex, args.Entries)
		} else {
			// 如果已经快照截断的日志长度大于收到的Entries的长度，说明已经匹配，可以直接返回成功
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, assume as a match. PLI: %d.", rf.me, args.PrevLogIndex)
			reply.Success = true
			return
		}
	}

	// 检查PrevLogIndex对应的日志项的Term与收到的PrevLogTerm是否匹配，如果不匹配，需要要求Leader重试
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.getEntry(args.PrevLogIndex).Term {
		Debug(dDrop, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		reply.XLen = len(rf.log) + rf.lastIncludedIndex
		reply.XTerm = rf.getEntry(args.PrevLogIndex).Term
		reply.XIndex, _ = rf.getBoundsWithTerm(reply.XTerm)
		return
	}

	// 根据收到的Entries更新日志
	for i, entry := range args.Entries {
		if rf.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.getSlice(1+rf.lastIncludedIndex, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}

	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)

	// 如果收到的Entries长度大于0，持久化日志
	if len(args.Entries) > 0 {
		rf.persist()
	}

	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher LC at T%d, updating commitIndex. (%d < %d)",
			rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}

	reply.Success = true
}

// 向其他节点发送心跳或日志条目
func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())
	lastLogIndex, _ := rf.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= rf.lastIncludedIndex {
			// current leader does not have enough log to sync the outdated peer,
			// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
			rf.sendSnapshot(peer)
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			args.Entries = rf.getSlice(nextIndex, lastLogIndex+1)
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

// 提交日志条目
func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)

		// 检查回复的任期是否小于当前任期，如果是，则说明回复的任期已过时，直接返回
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}

		// 检查当前任期是否与请求参数的任期相等，如果不相等，则说明在请求发送过程中，任期已经发生变化，直接返回
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}

		// 检查回复的任期是否大于当前任期，如果是，则更新当前任期并转换为 Follower
		if rf.checkTerm(reply.Term) {
			return
		}

		// 如果成功，为 Follower 更新 nextIndex 和 matchIndex
		if reply.Success {
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])

			// 根据多数派共识更新 commitIndex
			for N := rf.lastIncludedIndex + len(rf.log); N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm; N-- {
				count := 1
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}
					if matchIndex >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		} else {
			// 如果回复的 Success 字段为 false

			// 如果回复的 XTerm 为 -1，表示在 nextIndex 之前的日志已经全部匹配，更新 nextIndex 为回复的 XLen+1
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				// 如果回复的 XTerm 不为 -1，则根据回复的 XTerm 查找最后一条匹配该任期的日志条目的索引

				// 获取具有指定任期的日志条目的上下界索引
				_, maxIndex := rf.getBoundsWithTerm(reply.XTerm)
				if maxIndex != -1 {
					rf.nextIndex[server] = maxIndex
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}

			// 获取最后一条日志条目的索引和任期
			lastLogIndex, _ := rf.lastLogInfo()
			nextIndex := rf.nextIndex[server]

			// 如果 nextIndex 小于等于 lastIncludedIndex，说明需要发送快照
			if nextIndex <= rf.lastIncludedIndex {
				rf.sendSnapshot(server)
			} else if lastLogIndex >= nextIndex {
				// 如果 lastLogIndex 大于等于 nextIndex，说明日志不一致，重新发送日志条目

				Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
				newArg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
					Entries:      rf.getSlice(nextIndex, lastLogIndex+1),
				}
				go rf.leaderSendEntries(newArg, server)
			}
		}
	}
}

// >>>>>>>>>> 2B 日志同步部分实现 >>>>>>>>>>

// <<<<<<<<<< 2C 持久化部分实现 <<<<<<<<<<

// 放入持久化数据
func (rf *Raft) persist() {
	Debug(dPersist, "S%d Saving persistent state to stable storage at T%d.", rf.me, rf.currentTerm)

	// 编码并保存当前信息
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}

	// 将信息送入Persistor中
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 从持久化数据中读取信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	Debug(dPersist, "S%d Restoring previously persisted state at T%d.", rf.me, rf.currentTerm)

	// 恢复持久化之前的状态
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 调试信息
	if err := d.Decode(&rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.log); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.log\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedIndex\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedTerm\". err: %v, data: %s", err, data)
	}
}

// >>>>>>>>>> 2C 持久化部分实现 >>>>>>>>>>

// <<<<<<<<<< 2D 日志压缩及快照部分 <<<<<<<<<<

type InstallSnapshotArgs struct {
	Term              int    // 领导者任期
	LeaderId          int    // 领导者ID，以便于跟随者重定向请求
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int // 当前任期，用于领导者更新自己
}

// 状态机进行安装条件协商
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d Installing the snapshot. LLI: %d, LLT: %d", rf.me, lastIncludedIndex, lastIncludedTerm)
	lastLogIndex, _ := rf.lastLogInfo()

	// 如果快照的最后日志条目索引小于当前日志条目索引，忽略快照
	if rf.commitIndex >= lastIncludedIndex {
		Debug(dSnap, "S%d Log entries is already up-to-date with the snapshot. (%d >= %d)", rf.me, rf.commitIndex, lastIncludedIndex)
		return false
	}

	// 如果快照的最后日志条目索引大于当前日志条目索引，截断日志
	if lastLogIndex >= lastIncludedIndex {
		rf.log = rf.getSlice(lastIncludedIndex+1, lastLogIndex+1)
	} else {
		// 如果不是，清空日志
		rf.log = []LogEntry{}
	}

	// 更新状态机相关参数
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = snapshot

	// 将快照数据写入持久化存储
	rf.persistAndSnapshot(snapshot)
	return true
}

// 进行快照的创建
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.lastLogInfo()

	// 如果快照中包含的最后日志条目的索引值大于等于当前索引，则说明已经进行过快照
	if rf.lastIncludedIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludedIndex, index)
		return
	}

	// 如果提交的索引小于当前索引，未提交的日志无法进行快照
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}

	// 切片生成所需要的新日志，并更新状态机相关参数
	newLog := rf.getSlice(index+1, lastLogIndex+1)
	newLastIncludeTerm := rf.getEntry(index).Term
	rf.lastIncludedTerm = newLastIncludeTerm
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot

	// 将快照数据写入持久化存储
	rf.persistAndSnapshot(snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()         // 加锁，保证并发安全
	defer rf.mu.Unlock() // 解锁，在函数执行完后释放锁
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)

	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return // 当前任期大于请求任期，重置当前任期为请求发来的Leader任期，拒绝安装快照请求
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout()) // 重置选举超时时间，等待下一个可能的心跳超时

	if rf.waitingIndex >= args.LastIncludedIndex {
		Debug(dSnap, "S%d A newer snapshot already exists, rejecting install snapshot request. (%d <= %d)",
			rf.me, args.LastIncludedIndex, rf.waitingIndex)
		return // 已存在更新的快照，拒绝安装快照请求
	}
	rf.leaderId = args.LeaderId
	rf.waitingSnapshot = args.Data           // 设置等待中的快照数据
	rf.waitingIndex = args.LastIncludedIndex // 设置等待中的快照索引
	rf.waitingTerm = args.LastIncludedTerm   // 设置等待中的快照任期
}

func (rf *Raft) sendSnapshot(server int) {
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	// 设置快照安装请求参数
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	go rf.leaderSendSnapshot(args, server)
}

// 发送快照安装请求
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 由Leader发送快照
func (rf *Raft) leaderSendSnapshot(args *InstallSnapshotArgs, server int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()         // 加锁，保证并发安全
		defer rf.mu.Unlock() // 解锁，在函数执行完后释放锁
		Debug(dSnap, "S%d <- S%d Received install snapshot reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid install snapshot reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return // 如果回复的任期比当前节点的任期更低，则忽略回复
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the install snapshot request, install snapshot reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return // 如果安装快照请求后当前节点的任期发生了改变，则忽略回复
		}
		rf.checkTerm(reply.Term)                                     // 检查并更新当前节点的任期
		newNext := args.LastIncludedIndex + 1                        // 计算新的nextIndex
		newMatch := args.LastIncludedIndex                           // 计算新的matchIndex
		rf.matchIndex[server] = max(newMatch, rf.matchIndex[server]) // 更新matchIndex
		rf.nextIndex[server] = max(newNext, rf.nextIndex[server])    // 更新nextIndex
	}
}

// 持久化和快照
func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	Debug(dSnap, "S%d Saving persistent state and service snapshot to stable storage at T%d.", rf.me, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 对需要持久化的快照数据进行编码，包括了快照中包含的最后日志条目
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// >>>>>>>>>> 2D 日志压缩及快照部分 >>>>>>>>>>
