package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int   // 当前leader的ID
	clerkId  int64 // clerk的唯一ID
	seqId    int   // 请求的序列号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk) // 创建一个新的Clerk对象
	ck.servers = servers
	ck.clerkId = nrand() // 为Clerk对象的clientId字段生成一个随机数
	ck.seqId = -1        // 将Clerk对象的seqId字段设置为-1
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,             // 待获取的键值对的键
		ClerkId: ck.clerkId,      // Clerk 的唯一标识符
		SeqId:   ck.allocSeqId(), // 分配的序列号
	}

	reply := GetReply{}
	server := ck.leaderId
	for {
		DPrintf("[Clerk-%d] call [Get] request key=%s , SeqId=%d, server=%d", ck.clerkId, key, args.SeqId, server%len(ck.servers))
		ok := ck.SendGet(server%len(ck.servers), &args, &reply) // 发送 Get 请求给指定的服务器
		if ok {
			if reply.Err == ErrWrongLeader { // 如果收到了 ErrWrongLeader 错误，表示当前服务器不是 Leader
				server += 1
				DPrintf("[Clerk-%d] ErrWrongLeader, retry server=%d, args=%v", ck.clerkId, server%len(ck.servers), args)
				continue // 重试下一个服务器
			}
			ck.leaderId = server // 更新 Leader 的标识符
			DPrintf("[Clerk-%d] call [Get] response server=%d reply=%v, args=%v", ck.clerkId, server%len(ck.servers), reply, args)
			break // 获取到响应，退出循环
		} else {
			server += 1
		}
		time.Sleep(50 * time.Millisecond) // 等待一段时间后继续重试
	}

	return reply.Value // 返回获取到的键值对的值
}

func (ck *Clerk) allocSeqId() int {
	// 为 Clerk 结构体分配递增的序列号（seqId）
	ck.seqId += 1
	return ck.seqId
}

func (ck *Clerk) PutAppend(key string, value string, opName string) {
	args := PutAppendArgs{
		Key:     key,             // 待写入或追加的键值对的键
		Value:   value,           // 待写入或追加的键值对的值
		Op:      opName,          // 操作类型，可以是 "Put" 或 "Append"
		ClerkId: ck.clerkId,      // Clerk 的唯一标识符
		SeqId:   ck.allocSeqId(), // 分配的序列号
	}
	reply := PutAppendReply{}
	server := ck.leaderId
	for {
		DPrintf("[Clerk-%d] call [PutAppend] request key=%s value=%s op=%s, seq=%d, server=%d", ck.clerkId, key, value, opName, args.SeqId, server%len(ck.servers))
		ok := ck.SendPutAppend(server%len(ck.servers), &args, &reply) // 发送 PutAppend 请求给指定的服务器
		if ok {
			if reply.Err == ErrWrongLeader { // 如果收到了 ErrWrongLeader 错误，表示当前服务器不是 Leader
				server += 1
				time.Sleep(50 * time.Millisecond)
				DPrintf("[Clerk-%d] call [PutAppend] faild, try next server id =%d ... retry args=%v", ck.clerkId, server, args)
				continue // 重试下一个服务器
			}
			ck.leaderId = server // 更新 Leader 的标识符
			DPrintf("[Clerk-%d] call [PutAppend] response server=%d, ... reply = %v, args=%v", ck.clerkId, server%len(ck.servers), reply, args)
			break // 获取到响应，退出循环
		} else {
			server += 1
			DPrintf("[Clerk][PutAppend] %d faild, call result=false, try next server id =%d ... retry reply=%v", ck.clerkId, server, reply)
		}
		time.Sleep(50 * time.Millisecond) // 等待一段时间后继续重试
	}
}

func (ck *Clerk) SendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) SendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
