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

// 返回指定任期日志的第一和最后一个索引
func (logEntries LogEntries) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {

	// 如果任期为0，均返回0
	if term == 0 {
		return 0, 0
	}

	// 初始化并获取
	minIndex, maxIndex = math.MaxInt, -1
	for i := 1; i <= len(logEntries); i++ {
		if logEntries.getEntry(i).Term == term {
			minIndex = min(minIndex, i)
			maxIndex = max(maxIndex, i)
		}
	}

	// 若无该索引，返回两个-1
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
func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {

		// 定期应用日志，直到最后提交索引为止
		rf.mu.Lock()

		// 使用切片存储所有要应用的已提交信息
		appliedMsgs := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			// 准备相关数据
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
			Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}
		rf.mu.Unlock()

		// 送入状态机中
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

// 启动Raft实例的命令提交过程
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是Leader，不执行此进程
	if rf.state != Leader {
		return -1, -1, false
	}

	// 创建新的日志项
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}

	// 将日志项追加到服务器的日志中
	rf.log = append(rf.log, logEntry)
	index = len(rf.log)
	term = rf.currentTerm
	Debug(dLog, "S%d Add command at T%d。LI: %d, Command: %v\n", rf.me, term, index, command)

	// 持久化日志
	rf.persist()

	// 发送日志项给其他服务器
	rf.sendEntries(false)

	return index, term, isLeader
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

	// 进行初始化
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.setElectionTimeout(randHeartbeatTimeout())
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 从持久化状态中进行初始化
	rf.readPersist(persister.ReadRaftState())

	// 对于每个peer，初始化nextIndex为日志长度+1
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log) + 1
	}

	// 获取最后一条日志的索引和任期号
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
	Debug(dClient, "S%d 启动于 T%d。LLI: %d, LLT: %d。", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)

	// 启动定时器协程来开始选举
	go rf.ticker()

	// 定期将日志应用到commitIndex之前的位置，以确保状态机是最新的。
	go rf.applyLogsLoop(applyCh)

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
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
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

	// 对自身的LeaderId进行更新
	rf.leaderId = args.LeaderId

	// 如果日志不包含在prevLogIndex处的任期或prevLogTerm不匹配，拒绝请求
	// 拒绝后，Leader会递减nextIndex并在自身进行重试直至匹配
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.log.getEntry(args.PrevLogIndex).Term {
		Debug(dLog2, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		reply.XLen = len(rf.log)
		reply.XTerm = rf.log.getEntry(args.PrevLogIndex).Term
		// getBoundsWithTerm返回满足给定任期的第一个和最后一个日志条目的索引
		reply.XIndex, _ = rf.log.getBoundsWithTerm(reply.XTerm)
		return
	}

	// Leader匹配成功后直接发来后续所有已提交的日志
	// Follower使用Leader发来的Entries完全替换自己的错误的
	for i, entry := range args.Entries {
		if rf.log.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			Debug(dLog2, "S%d Running into conflict with existing entries at T%d. conflictLogs: %v, startIndex: %d.",
				rf.me, rf.currentTerm, args.Entries[i:], i+1+args.PrevLogIndex)
			rf.log = append(rf.log.getSlice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)

	// 如果日志长度大于0，将其持久化
	if len(args.Entries) > 0 {
		rf.persist()
	}

	// 如果leaderCommit > commitIndex，令commitIndex等于leaderCommit和新日志条目索引值中较小的一个
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

	// 随机设置心跳和选举超时时间
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())

	// 获取最后一条日志的索引和任期
	lastLogIndex, _ := rf.log.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// 打包准备发送的信息
		nextIndex := rf.nextIndex[peer]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}

		// 如果最后一条日志的索引大于等于nextIndex，说明Leader和Follower的日志不一致，发送日志条目
		if lastLogIndex >= nextIndex {
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

// 提交日志条目
func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)

		// 如果回复的任期小于当前任期，则回复无效，不进行任何操作
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}

		// 如果当前任期发生变化，则丢弃回复
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded."+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}

		// 检查回复的任期是否比当前任期新，如果是则更新当前任期并返回。
		if rf.checkTerm(reply.Term) {
			return
		}

		// 如果追加日志项成功，则更新Follower的nextIndex和matchIndex。
		// 如果存在一个N，满足N > commitIndex，且大多数的matchIndex[i] ≥ N，且log[N].term == currentTerm，
		// 则设置commitIndex = N，实现日志的提交。
		if reply.Success {
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])

			for N := len(rf.log); N > rf.commitIndex && rf.log.getEntry(N).Term == rf.currentTerm; N-- {
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
		} else { // 如果追加日志项失败，则根据回复的信息进行相应的处理。

			// Follower日志过短
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				_, maxIndex := rf.log.getBoundsWithTerm(reply.XTerm)

				if maxIndex != -1 { // Leader存在XTerm
					rf.nextIndex[server] = maxIndex
				} else { // Leader不存在XTerm
					rf.nextIndex[server] = reply.XIndex
				}
			}
			lastLogIndex, _ := rf.log.lastLogInfo()
			nextIndex := rf.nextIndex[server]

			// 对发生日志错误或缺少的Follower一次性补充日志
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
}

// >>>>>>>>>> 2C 持久化部分实现 >>>>>>>>>>

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
