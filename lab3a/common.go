package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutAppendArgs struct {
	Key     string // 键名
	Value   string // 值
	Op      string // 操作类型（Put 或 Append）
	ClerkId int64  // 客户端 ID
	SeqId   int    // 操作序号
}

type PutAppendReply struct {
	Err Err // 错误类型
}

type GetArgs struct {
	Key     string // 键名
	ClerkId int64  // 客户端 ID
	SeqId   int    // 操作序号
}

type GetReply struct {
	Err   Err    // 错误类型
	Value string // 值
}
