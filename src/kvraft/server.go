package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Op是请求数据结构，OpPA代表该请求操作类型
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpPA    string
	Key     string
	Val     string
	ClerkId int
	Seq     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb        map[string]string
	lastSeqs    map[int]int     // 记录每个clerk已经提交的日志的序列号（op中的序列号，与日志下标不一样）
	agreeChs    map[int]chan Op // 每个日志下标对应的通知rpc结束的管道
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		OpPA:    "Get",
		Key:     args.Key,
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getAgreeCh(index)
	var cmd_ Op
	select {
	case cmd_ = <-ch:
		close(ch)
	case <-time.After(1000 * time.Millisecond): // 超时重试
		reply.Err = ErrWrongLeader
		return
	}
	if !isSameOp(cmd_, cmd) {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Value = kv.kvdb[args.Key]
	reply.Err = OK
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		OpPA:    args.OpPA,
		Key:     args.Key,
		Val:     args.Value,
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
	}
	index, _, isLeader := kv.rf.Start(cmd)
	// fmt.Printf("server层 %d send cmd to raft层\n", kv.me)
	// fmt.Println(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// fmt.Printf("server层 %d, 不是leader，返回clerk ErrWrongLeader\n", kv.me)
		return
	}
	ch := kv.getAgreeCh(index)
	var cmd_ Op
	select {
	case cmd_ = <-ch:
		close(ch)
	case <-time.After(1000 * time.Millisecond): // 超时重试
		reply.Err = ErrWrongLeader
		// fmt.Printf("server层 %d, cmd 超时未同步，返回clerk ErrWrongLeader\n", kv.me)
		// fmt.Println(cmd)
		return
	}
	// 网络分区修复前，某个term更小的leader正好start(cmd)，然后cmd还未被同步完成即还未提交，
	// 网络分区修复后，新的cmd覆盖了这个index上原有的cmd，然后通过applych传回了该cmd，
	// 此时该rpc handler发起时的需同步的cmd与applych传回的不是同一个cmd，之前的需同步的cmd实际未同步，所以需要告知clerk重试
	if !isSameOp(cmd_, cmd) { // 所以需要判断cmd_与cmd是否相同
		reply.Err = ErrWrongLeader
		// fmt.Printf("server层 %d, raft返回的日志与server层下发的日志不同，返回clerk ErrWrongLeader\n", kv.me)
		// fmt.Println(cmd)
		return
	}
	// fmt.Printf("server层 %d, cmd 已应用到状态机\n", kv.me)
	// fmt.Println(cmd)
	reply.Err = OK
	return
}
func isSameOp(a, b Op) bool {
	return a.ClerkId == b.ClerkId && a.Seq == b.Seq && a.OpPA == b.OpPA && a.Key == b.Key && a.Val == b.Val
}
func (kv *KVServer) WaitAgree() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			// fmt.Printf("server层%d 接收到 raft层传来的msg\n", kv.me)
			// fmt.Println(msg)
			if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.readSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
			} else if msg.CommandValid {
				cmd := msg.Command.(Op)
				kv.mu.Lock()
				lastSeqs, ok := kv.lastSeqs[cmd.ClerkId]
				kv.lastApplied = msg.CommandIndex
				if !ok || lastSeqs < cmd.Seq {
					// 应用到状态机中
					kv.applyToStateMachine(cmd)
					kv.lastSeqs[cmd.ClerkId] = cmd.Seq
				}
				kv.mu.Unlock()
				kv.getAgreeCh(msg.CommandIndex) <- cmd
				// fmt.Printf("server层%d 已对raft传回来的消息做出重复判断,返回rpc handler\n", kv.me)
			}
		}
	}
}
func (kv *KVServer) getAgreeCh(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.agreeChs[idx]
	if !ok {
		ch = make(chan Op, 1)
		kv.agreeChs[idx] = ch
	}
	return ch
}
func (kv *KVServer) applyToStateMachine(cmd Op) {
	op := cmd.OpPA // 命令操作类型
	switch op {
	case "Put":
		kv.kvdb[cmd.Key] = cmd.Val
	case "Append":
		kv.kvdb[cmd.Key] += cmd.Val
	}
	// fmt.Printf("cmd ")
	// fmt.Println(cmd, "apply")
}

// 生成server层的状态快照
func (kv *KVServer) buildSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvdb)
	e.Encode(kv.lastSeqs)
	data := w.Bytes()
	return data
}
func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvdb map[string]string
	var lastSeqs map[int]int
	if d.Decode(&kvdb) != nil || d.Decode(&lastSeqs) != nil {
		fmt.Printf("获取persist信息失败\n")
	} else {
		kv.kvdb = kvdb
		kv.lastSeqs = lastSeqs
	}
}
func (kv *KVServer) applySnapshot(persister *raft.Persister) {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate <= persister.RaftStateSize() {
			index := kv.lastApplied
			snapshot := kv.buildSnapshot()
			kv.rf.Snapshot(index, snapshot)
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvdb = make(map[string]string, 0)
	kv.agreeChs = make(map[int]chan Op, 0)
	kv.lastSeqs = make(map[int]int, 0)

	// You may need initialization code here.
	go kv.WaitAgree()
	return kv
}
