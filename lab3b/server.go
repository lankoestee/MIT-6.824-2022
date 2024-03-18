package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	raft "6.824/lab2d"
	"6.824/labgob"
	"6.824/labrpc"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key      string // 操作的键名
	Value    string // 操作的值
	Command  string // 操作的命令类型（Get、Put、Append）
	ClientId int64  // 客户端的唯一标识符
	SeqId    int    // 操作的序列号
	Server   int    // 服务端的标识符
}

type KVState struct {
	CKs       map[int64]int     // 客户端最新操作序列号的映>射表
	DataSouce map[string]string // 存储键值对的数据源
}

type ClerkOps struct {
	seqId       int     // 客户端当前操作序列号
	getCh       chan Op // Get操作的通道
	putAppendCh chan Op // Put和Append操作的通道
	msgUniqueId int     // RPC等待消息的唯一标识符
}

func (ck *ClerkOps) GetCh(command string) chan Op {
	switch command {
	case "Put": // 如果命令是"Put"，返回Put和Append操作的通>道
		return ck.putAppendCh
	case "Append": // 如果命令是"Append"，返回Put和Append操>作的通道
		return ck.putAppendCh
	default: // 否则，返回Get操作的通道
		return ck.getCh
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // 通过Kill()方法设置

	maxraftstate int // 当日志增长到一定大小时进行快照

	dataSource map[string]string   // 存储键值对的数据源
	messageMap map[int64]*ClerkOps // 客户端ID与ClerkOps结构体的映射表
	messageCh  chan raft.ApplyMsg  // 用于接收Raft层的ApplyMsg消息的通道
	persister  *raft.Persister     // 持久化存储
}

func (kv *KVServer) WaitApplyMsgByCh(ch chan Op, ck *ClerkOps) (Op, Err) {
	startTerm, _ := kv.rf.GetState()                // 获取>当前服务器的任期号
	timer := time.NewTimer(1000 * time.Millisecond) // 创建>一个定时器，设置超时时间为1秒
	for {
		select {
		case Msg := <-ch: // 从通道接收到消息
			return Msg, OK // 返回接收到的消息和OK错误码
		case <-timer.C: // 定时器超时
			curTerm, isLeader := kv.rf.GetState()  // 获取当前服务器的任期号和领导状态
			if curTerm != startTerm || !isLeader { // 如果当前任期号不等于开始任期号，或者当前不是领导者
				kv.mu.Lock()
				ck.msgUniqueId = 0 // 将ClerkOps结构体的消息唯一标识符重置为0
				kv.mu.Unlock()
				return Op{}, ErrWrongLeader // >返回空的操作和ErrWrongLeader错误码
			}
			timer.Reset(1000 * time.Millisecond) // 重新设置定时器超时时间为1秒
		}
	}
}

func (kv *KVServer) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// 等待200毫秒
	// 如果通知超时，则忽略，因为客户端可能已经发送请求到另一个服务器
	timer := time.NewTimer(200 * time.Millisecond) // 创建一个定时器，设置超时时间为200毫秒
	select {
	case ch <- Msg: // 将消息发送到通道
		return
	case <-timer.C: // 定时器超时
		DPrintf("[KVServer-%d] NotifyApplyMsgByCh Msg=%v, timeout", kv.me, Msg) // 打印超时日志
		return
	}
}

func (kv *KVServer) GetCk(ckId int64) *ClerkOps {
	ck, found := kv.messageMap[ckId] // 根据客户端ID从映射表中获取ClerkOps结构体
	if !found {                      // 如果未找到对应的ClerkOps结构体
		ck = new(ClerkOps)                               // 创建一个新的ClerkOps结构体
		ck.seqId = 0                                     // 将序列号初始化为0
		ck.getCh = make(chan Op)                         // 创建Get操作的通道
		ck.putAppendCh = make(chan Op)                   // 创建Put和Append操作的通道
		kv.messageMap[ckId] = ck                         // 将新创建的ClerkOps结构体添加到映射表中
		DPrintf("[KVServer-%d] Init ck %d", kv.me, ckId) // 打印日志，表示初始化了新的ClerkOps结构体
	}
	return kv.messageMap[ckId] // 返回对应的ClerkOps结构体
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId)                              // 获取客户端的ClerkOps结构体
	DPrintf("[KVServer-%d] Received Req Get %v", kv.me, args) // 打印日志，表示收到Get请求
	// 开始一个命令
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Command:  "Get",
		ClientId: args.ClerkId,
		SeqId:    args.SeqId,
		Server:   kv.me,
	})
	if !isLeader { // 如果当前不是领导者
		reply.Err = ErrWrongLeader // 设置错误码为ErrWrongLeader
		ck.msgUniqueId = 0         // 将ClerkOps结构体的消息唯一标识符重置为0
		kv.mu.Unlock()
		return
	}
	DPrintf("[KVServer-%d] Received Req Get %v, waiting logIndex=%d", kv.me, args, logIndex) // 打印日志，表示等待日志提交
	ck.msgUniqueId = logIndex                                                                // 将当前命令的日志索引设置为ClerkOps结构体的消息唯一标识符
	kv.mu.Unlock()
	// 解析Op结构体
	getMsg, err := kv.WaitApplyMsgByCh(ck.getCh, ck) // 等待从通道接收到Get操作的结果
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer-%d] Received Msg [Get] args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, getMsg) // 打印日志，表示收到了Get操作的结果
	reply.Err = err
	if err != OK {
		// 领导者发生变更，返回ErrWrongLeader错误码
		return
	}

	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
		return
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
		DPrintf("[KVServer-%d] Excute Get %s is %s", kv.me, getMsg.Key, reply.Value) // 打印日志，表示执行了Get操作
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId) // 获取客户端的PutAppendArgs结构体
	// 已经处理过了
	if ck.seqId > args.SeqId { // 如果PutAppendArgs结构体的序列号大于请求的序列号
		kv.mu.Unlock()
		reply.Err = OK // 设置错误码为OK
		return
	}
	DPrintf("[KVServer-%d] Received Req PutAppend %v, SeqId=%d ", kv.me, args, args.SeqId) // 打印日志，表示收到了PutAppend请求
	// 开始一个命令
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Op,
		ClientId: args.ClerkId,
		SeqId:    args.SeqId,
		Server:   kv.me,
	})
	if !isLeader { // 如果当前不是领导者
		reply.Err = ErrWrongLeader // 设置错误码为ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck.msgUniqueId = logIndex                                                                      // 将当前命令的日志索引设置为ClerkOps结构体的消息唯一标识符
	DPrintf("[KVServer-%d] Received Req PutAppend %v, waiting logIndex=%d", kv.me, args, logIndex) // 打印日志，表示等待日志提交
	kv.mu.Unlock()
	// 第二步：等待通道
	reply.Err = OK                                      // 设置错误码为OK
	Msg, err := kv.WaitApplyMsgByCh(ck.putAppendCh, ck) // 等待从通道接收到PutAppend操作的结果
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer-%d] Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, Msg) // 打印日志，表示收到了PutAppend操作的结果
	reply.Err = err
	if err != OK {
		DPrintf("[KVServer-%d] leader change args=%v, SeqId=%d", kv.me, args, args.SeqId) // 打印日志，表示领导者发生了变更
		return
	}
}

func (kv *KVServer) needSnapshot() bool {
	return kv.persister.RaftStateSize()/4 >= kv.maxraftstate && kv.maxraftstate != -1
}

func (kv *KVServer) processMsg() {
	for {
		// 从通道接收到消息
		applyMsg := <-kv.applyCh
		Msg := applyMsg.Command.(Op)
		DPrintf("[KVServer-%d] Received Msg from channel. Msg=%v", kv.me, applyMsg)

		// 检查是否需要读取快照，如果需要，则进行快照
		if applyMsg.SnapshotValid {
			kv.readKVState(applyMsg.Snapshot)
			continue
		}

		kv.mu.Lock()
		ck := kv.GetCk(Msg.ClientId) // 获取客户端的ClerkOps结构体
		if Msg.SeqId > ck.seqId {
			kv.mu.Unlock()
			continue
		}

		// 检查是否需要保存快照
		_, isLeader := kv.rf.GetState()
		if kv.needSnapshot() {
			kv.saveKVState(applyMsg.CommandIndex - 1)
		}

		// 检查是否需要通知
		needNotify := ck.msgUniqueId == applyMsg.CommandIndex
		//DPrintf("[KVServer-%d] msg=%v, isleader=%v, ck=%v", kv.me, Msg, ck)
		if Msg.Server == kv.me && isLeader && needNotify { // 如果当前服务器是领导者，并且需要通知客户端
			// 通知通道并重置时间戳
			ck.msgUniqueId = 0
			DPrintf("[KVServer-%d] Process Msg %v finish, ready send to ck.Ch, SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader) // 打印日志，表示处理完成并准备发送到ClerkOps结构体的通道
			kv.NotifyApplyMsgByCh(ck.GetCh(Msg.Command), Msg)                                                                              // 通过通道通知客户端
			DPrintf("[KVServer-%d] Process Msg %v Send to Rpc handler finish SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader)   // 打印日志，表示发送到Rpc处理程序完成
		}

		if Msg.SeqId < ck.seqId { // 如果日志的序列号小于ClerkOps结构体的序列号
			DPrintf("[KVServer-%d] Ignore Msg %v,  Msg.SeqId < ck.seqId", kv.me, applyMsg) // 打印日志，表示忽略该日志
			kv.mu.Unlock()
			continue
		}

		switch Msg.Command { // 根据命令类型执行相应的操作
		case "Put":
			kv.dataSource[Msg.Key] = Msg.Value                                                                          // 执行Put操作，将键值对写入数据源
			DPrintf("[KVServer-%d] Excute CkId=%d Put Msg=%v, kvdata=%v", kv.me, Msg.ClientId, applyMsg, kv.dataSource) // 打印日志，表示执行了Put操作
		case "Append":
			DPrintf("[KVServer-%d] Excute CkId=%d Append Msg=%v kvdata=%v", kv.me, Msg.ClientId, applyMsg, kv.dataSource) // 打印日志，表示执行了Append操作
			kv.dataSource[Msg.Key] += Msg.Value                                                                           // 执行Append操作，将值追加到键对应的现有值后面
		case "Get":
			DPrintf("[KVServer-%d] Excute CkId=%d Get Msg=%v kvdata=%v", kv.me, Msg.ClientId, applyMsg, kv.dataSource) // 打印日志，表示执行了Get操作
		}
		ck.seqId = Msg.SeqId + 1 // 更新ClerkOps结构体的序列号
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("%d Received Kill Command, logsize=%d, kv data=%v", kv.me, kv.persister.RaftStateSize(), kv.dataSource)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readKVState(data []byte) {
	if data == nil || len(data) < 1 {
		return // 如果数据为空或长度小于1，则直接返回
	}

	DPrintf("[KVServer-%d] read size=%d", kv.me, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	cks := make(map[int64]int)            // 创建一个map，用于存储客户端ID和序列号
	dataSource := make(map[string]string) // 创建一个map，用于存储数据源
	if d.Decode(&cks) != nil ||           // 解码客户端ID和序列号，如果解码失败，则输出错误信息
		d.Decode(&dataSource) != nil {
		DPrintf("[readKVState] decode failed ...")
	} else {
		for ckId, seqId := range cks { // 遍历客户端ID和序列号的map
			kv.mu.Lock()
			ck := kv.GetCk(ckId) // 根据客户端ID获取客户端
			ck.seqId = seqId     // 更新客户端的序列号
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		kv.dataSource = dataSource                                                                            // 更新数据源
		DPrintf("[KVServer-%d] readKVState messageMap=%v dataSource=%v", kv.me, kv.messageMap, kv.dataSource) // 打印读取的数据和更新后的数据
		kv.mu.Unlock()
	}
}

func (kv *KVServer) saveKVState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int) // 创建一个map，用于存储客户端ID和序列号
	for ckId, ck := range kv.messageMap {
		cks[ckId] = ck.seqId // 将客户端ID和序列号添加到map中
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	kv.rf.Snapshot(index, w.Bytes()) // 将数据写入快照
	DPrintf("[KVServer-%d] Size=%d", kv.me, kv.persister.RaftStateSize())
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate // 该参数由测试代码传入

	kv.applyCh = make(chan raft.ApplyMsg, 1000)           // 创建一个通道，用于接收Raft层的ApplyMsg消息
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 创建一个Raft结构体
	kv.mu.Lock()
	DPrintf("Start KVServer-%d", me)

	// 进行一系列初始化
	kv.dataSource = make(map[string]string)
	kv.messageMap = make(map[int64]*ClerkOps)
	kv.messageCh = make(chan raft.ApplyMsg, 1000)
	kv.persister = persister
	kv.mu.Unlock()

	// 读取被持久化的快照数据
	kv.readKVState(kv.persister.ReadSnapshot())
	go kv.processMsg()
	return kv
}
