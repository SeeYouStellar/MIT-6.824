package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	"6.824/labgob"

	//	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const ( // 单位ms
	hbInterval = 100
	rvStart    = 500
	rvEnd      = 1500
	//rvTimeout = 300
)

// 角色映射（int-string)
func roleMap(role int) string {
	switch role {
	case 0:
		return "follower"
	case 1:
		return "candidate"
	case 2:
		return "leader"
	}
	return "XXX"
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// 定义日志条目
type Log struct {
	Index   int         // 索引
	Command interface{} // 日志命令
	Term    int         // 当前日志条目所在term
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's role
	wg        sync.WaitGroup
	cd        sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted role
	applyCh   chan ApplyMsg       // 外部关联raft的channel
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// role a Raft server must maintain.
	role int // 2:leader / 1:candidate / 0:follower
	ch   chan string

	currentTerm int   // 当前服务器看到的最新term
	votedFor    int   // 投出的选票对应的候选人ID
	log         []Log // log条目

	commitIndex int // 已知被提交的最高log条目
	lastApplied int // 应用到状态机的上一条log条目

	nextIndex  []int // 要送到服务器的下一个log Index (each)
	matchIndex []int // 已知被备份到服务器的最新log Index (each)

	lastIncludedIndex int    // 快照的最大log index
	lastIncludedTerm  int    // 快照的最后一条log term
	snapshotOffset    int    // 快照可能分块（偏移量）
	snapshot          []byte // 快照字节数据
}

// 获取rf最后一个log的index,term
func (rf *Raft) lastLog() (int, int) {
	if len(rf.log) <= 1 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.role == 2 {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent role to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//持久化当前term以及是否给其他结点投过票，避免同一个term多次投票的情况
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	return w.Bytes()
}

// restore previously persisted role.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any role?
		fmt.Println("重启找不到 persist data!")
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []Log
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		fmt.Printf("读persister错误！\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		fmt.Println("readPersist: ", currentTerm, voteFor, log, lastIncludedIndex)
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader的term
	LeaderID          int    // 用于follower为客户端重定向
	LastIncludedIndex int    // 快照所包含的最后index
	LastIncludedTerm  int    // 快照所包含的最后index对应的term
	Offset            int    // 块在快照中的字节偏移
	Data              []byte // 从offset处开始，快照块中的原始字节
	Done              bool   // 是否是最后块
}

type InstallSnapshotReply struct {
	Term int // 当前term(用于leader自我更新)
}

// InstallSnapshot RPC  通知follower提交快照装载的RPC，保证快照提交的一致性（当follower的log落后于leader的快照时）
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term || args.Data == nil { // 如果leader的term不是最新，则不执行
		return
	}
	if rf.currentTerm < args.Term { // 如果自己term不是最新则需重置角色为follower
		rf.role = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.commitIndex >= args.LastIncludedIndex {
		return
	} // 如果自己的提交index大于leader快照index，则不执行
	// 发送快照到应用层
	applyMsg := ApplyMsg{ // 封装apply报文
		SnapshotValid: true,      // 告诉应用状态机本次提交为快照
		Snapshot:      args.Data, // 提交快照与leader的一致
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm}
	//rf.mu.Unlock()
	go func() { // 异步向通道发送apply报文
		rf.applyCh <- applyMsg
	}()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { // 用于follower装载快照
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > lastIncludedIndex { // 如果当前raft的提交信息并不落后则拒绝执行
		return false
	}
	fmt.Printf("Server%d get CondInstallSnapshot!\n", rf.me)
	log := rf.log[0:1] // 清除前面的log，转为装载新快照
	log[0].Index = lastIncludedIndex
	log[0].Term = lastIncludedTerm
	log[0].Command = "snapshot"
	lastIndex, _ := rf.lastLog()
	if lastIndex > lastIncludedIndex { // 仅rfat.log长于快照要求长度时需保留后续log
		log = append(log, rf.log[lastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = log
	// 更新snapshot信息
	rf.snapshot = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	// 更新raft的提交信息
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot) // 持久化当前raft与快照状态
	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) { // 用于leader压缩log生成快照
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Server%d start snapshot! index=%d\n", rf.me, index)
	lastIndex, _ := rf.lastLog()
	if index <= rf.lastIncludedIndex || index > rf.lastApplied || index > lastIndex { // 如果快照截止index不合理则拒绝执行
		fmt.Printf("Server%d fail to snapshot\n", rf.me)
		fmt.Printf("lastIncludedIndex=%d lastApplied=%d lastIndex=%d\n", rf.lastIncludedIndex, rf.lastApplied, lastIndex)
		return
	}

	log := rf.log[0:1] // 压缩log截至index，生成快照信息
	log[0].Index = index
	log[0].Term = rf.log[index-rf.lastIncludedIndex].Term
	log[0].Command = "snapshot"
	log = append(log, rf.log[index-rf.lastIncludedIndex+1:]...) // 保留index后的log
	rf.log = log
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = log[0].Term

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot) // 持久化当前raft与快照状态
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) oneInstallSnapshot(server int, args InstallSnapshotArgs) {
	//fmt.Printf("sendInstallSnapshot!\n")
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term || rf.role != 2 {
		return
	}
	//发现更大的term，本结点是旧leader
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = 0
		rf.persist()
		return
	}
	//follower拒绝snapshot证明其commitIndex>lastIncludedIndex，接收也可以使得其commitIndex>lastIncludedIndex
	rf.matchIndex[server] = rf.lastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	matchIndexSlice := make([]int, len(rf.peers)) // 将matchIndex复制取出进行排序，取中位数即为commitIndex应该更新到的 值
	for index, matchIndex := range rf.matchIndex {
		matchIndexSlice[index] = matchIndex
	}
	sort.Slice(matchIndexSlice, func(i, j int) bool {
		return matchIndexSlice[i] < matchIndexSlice[j]
	})
	//fmt.Printf("matchIndexSlice: %v, newcommitIndex: %v, lastLogIndex: %v\n", mr.Any2String(matchIndexSlice), matchIndexSlice[rf.nPeers/2], rf.lastLogIndex())
	newCommitIndex := matchIndexSlice[len(rf.peers)/2]
	//不能提交不属于当前term的日志
	//DPrintf("id[%d] role[%v] snapshot commitIndex %v update to newcommitIndex %v, lastSnapshotIndex %v,  command: %v, matchIndex: %v\n", rf.me, rf.role, rf.commitIndex, newCommitIndex, rf.lastIncludeIndex, 0, mr.Any2String(rf.matchIndex))
	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
		//如果commitIndex比自己实际的日志长度还大，这时需要减小
		lastIndex, _ := rf.lastLog()
		if newCommitIndex > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = newCommitIndex
		}
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的term
	CandidateID  int // 候选人请求投票
	LastLogIndex int // 候选人的最后一个log条目号
	LastLogTerm  int // 候选人的最后一个log条目对应的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	//mu          sync.Mutex
	Term        int  // 系统当前term，用于候选人更新自己的term
	VoteGranted bool // true表示候选人获得投票
	VoteNum     int  // 候选人获取到的票数
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Server%d 收到候选人%d 的RequestVote\n", rf.me, args.CandidateID)
	if rf.role == 0 {
		go func() {
			select {
			case rf.ch <- "RequestVote":
				return
			case <-time.After(5 * hbInterval):
				return
			}
		}()
	}
	if args.Term < rf.currentTerm { // 如果rf自己的term比请求的候选人更新
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm { // 如果rf自己的term已经过期，则转换为follower并投票给候选人
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist() // 保存数据至persister
		rf.role = 0
		//reply.VoteGranted = true
	}
	lastIndex, lastTerm := rf.lastLog()
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID { // rf已投过票且不是投给该候选人
		fmt.Printf("Candidate%d请求失败！Server%d 已投票给Server%d\n", args.CandidateID, rf.me, rf.votedFor)
		reply.VoteGranted = false
	} else if rf.votedFor == -1 && (lastTerm < args.LastLogTerm || // rf没投票且候选人的最大logTerm>rf的
		lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) { // 或Term相同时候选人的log比rf长
		rf.votedFor = args.CandidateID // 投票给候选人
		rf.persist()                   // 保存数据至persister
		rf.role = 0                    // rf转变为follower
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 向所有Server发送RequestVote请求
func (rf *Raft) sendRV2All() {
	rf.mu.Lock()
	lastIndex, _ := rf.lastLog()
	rvArgs := RequestVoteArgs{ // 初始化rpc参数
		Term:         rf.currentTerm, // 候选人的term
		CandidateID:  rf.me,          // 候选人的ID
		LastLogIndex: lastIndex,      // 候选人的最后一个log条目号
		LastLogTerm:  setLogTerm(rf)} // rf.log[rf.commitIndex].Term, // 候选人的最后一个log条目对应的term
	term := rf.currentTerm
	rf.votedFor = rf.me // 候选人投票给自己
	rf.persist()        // 保存数据至persister
	allPeers := len(rf.peers)
	fmt.Printf("Server%d(%s %d) 向所有其他服务器发送RequestVote!\n", rf.me, roleMap(rf.role), term)
	rf.mu.Unlock()
	voteNum := 1
	for i := 0; i < allPeers; i++ { // 向每个服务器发送请求投票
		if i != rf.me {
			server := i
			go func() { // 并行发送RequestVote请求，在各自协程里接收反馈
				//fmt.Printf("Server%d(%s %d) 向Server%d sendRequestVote!\n", rf.me, roleMap(role), term, server)
				rvReply := RequestVoteReply{Term: term}
				rf.sendRequestVote(server, &rvArgs, &rvReply)
				rf.mu.Lock()
				if rvReply.VoteGranted { // 如果收到投票
					voteNum++
					fmt.Printf("Server%d(%s %d) 收到 Server%d 投票(%d/%d)!\n", rf.me, roleMap(rf.role), rf.currentTerm, server, voteNum, len(rf.peers))
					//fmt.Printf("VoteNum = %d\n", voteNum)
					if voteNum > len(rf.peers)/2 { // 如果收到的票数超过总数的一半则成为leader
						rf.role = 2
						lastIndex, _ = rf.lastLog()
						for i := 0; i < allPeers; i++ { // 对所有follower初始化
							rf.nextIndex[i] = lastIndex + 1 // 初始化为自己log的最后index的下一个
							rf.matchIndex[i] = 0            // 初始化为0，单调递增
						}
						rf.matchIndex[rf.me] = lastIndex
						//fmt.Printf("Server%d(%s %d) 初始化next,match!\n", rf.me, roleMap(rf.role), rf.currentTerm)
						go func() {
							select {
							case rf.ch <- "BecomeLeader":
								return
							case <-time.After(5 * hbInterval):
								return
							}
						}()
					}
				} else { // 如果没收到投票
					// TODO 无法解决在goroutine立即转变为跟随者，外部for无法影响(其实执行到此步时外部for应该已经结束)
					if rf.currentTerm < rvReply.Term { // 如果自己的term被更新
						fmt.Printf("Server%d(%s %d) term过期转变为Follower\n", rf.me, roleMap(rf.role), rf.currentTerm)
						rf.currentTerm = rvReply.Term // 更新到最新term值
						rf.role = 0
						rf.votedFor = -1
						rf.persist() // 保存数据至persister
					}
				}
				rf.mu.Unlock()
			}()
		}
	}
}

func setLogTerm(rf *Raft) int {
	_, lastTerm := rf.lastLog()
	if len(rf.log) == 0 {
		return 0
	}
	return lastTerm
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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

// AppendEntries RPC 结构定义（实现心跳）
type AppendEntriesArgs struct {
	Term         int   // leader的term
	LeaderID     int   // follower重定向client
	PreLogIndex  int   // 新条目之前的日志条目index
	PreLogTerm   int   // 新条目之前的日志条目term
	Entries      []Log // 需要存储的log条目
	LeaderCommit int   // leader的提交index
}

type AppendEntriesReply struct {
	//mu      sync.Mutex
	Term    int  // 当前term（用于leader更新自己）
	Success bool // 如果条目匹配先前logindex和termindex则返回true
}

// 服务器接收来自leader的AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Server%d 收到Leader%d 的sendAppendEntries!\n", rf.me, args.LeaderID)
	fmt.Printf("Server%d log=[%v] commitIndex=%d lastApplied=%d\n", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
	if args.Term < rf.currentTerm { // 如果leader的term已经过期 (AE1)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.role = 0 // 收到心跳就要转变为follower
		go func() { // 并行通知rf已收到心跳
			select {
			case rf.ch <- "AppendEntries":
			case <-time.After(5 * hbInterval):
			}
		}()
		if args.Term > rf.currentTerm { // 如果rf自己的term已经过期,则转变为follower (Rules All 2)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist() // 保存数据至persister
			return
		}
		lastIndex, _ := rf.lastLog()
		if args.PreLogIndex <= lastIndex { // len(rf.log)
			if args.PreLogTerm == rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term { // 如果rf跟leader最新log之前的log匹配 (reply.success)
				rf.log = rf.log[:args.PreLogIndex-rf.lastIncludedIndex+1] // 删除PreLogIndex后面的所有条目
				rf.log = append(rf.log, args.Entries...)                  // 将与leader不匹配的新条目附加在PreLogIndex之后
				rf.persist()                                              // 保存数据至persister
				reply.Success = true
				// 设置follower的最后提交logIndex = min(leader的提交logIndex,rf的最后logIndex) (AE5)
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit > lastIndex { // len(rf.log) {
						rf.commitIndex = lastIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					go rf.sendApplyMsg(rf.commitIndex)
					//rf.cd.Signal()
				} else if rf.commitIndex > rf.lastApplied {
					go rf.sendApplyMsg(rf.commitIndex)
				}
			} else { // 如果leader的最新log前的最后条目term跟rf的不匹配 (AE2)
				//fmt.Printf("args.PreLogTerm(%d) == rf.log[args.PreLogIndex(%d)-rf.lastIncludedIndex(%d)].Term\n", args.PreLogTerm, args.PreLogIndex, rf.lastIncludedIndex)
				reply.Success = false
			}
		} else { // 如果leader的PreLogIndex比rf的log长度还大
			//fmt.Printf("args.PreLogIndex=%d < %d+1\n", args.PreLogIndex, lastIndex)
			reply.Success = false
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向单个server发送AppendEntries RPC
func (rf *Raft) oneAppendEntries(server int, term int, commit int) {
	rf.mu.Lock()
	fmt.Printf("rf.nextIndex[%d]=%d, rf.lastIncludedIndex=%d\n", server, rf.nextIndex[server], rf.lastIncludedIndex)
	if rf.nextIndex[server] <= rf.lastIncludedIndex { // TODO 会出现 rf.lastIncludedIndex 增长过快的情况，导致下标越界
		rf.mu.Unlock()
		return
	}
	snextIndex := rf.nextIndex[server]
	lastIncludedIndex := rf.lastIncludedIndex
	rf.mu.Unlock()

	aeArgs := AppendEntriesArgs{
		Term:         term,                                        // leader的term
		LeaderID:     rf.me,                                       // follower重定向client
		PreLogIndex:  snextIndex - 1,                              // 新条目之前的日志条目index
		PreLogTerm:   rf.log[snextIndex-lastIncludedIndex-1].Term, // 新条目之前的日志条目term
		Entries:      []Log{},                                     // 需要存储的log条目
		LeaderCommit: commit}                                      // leader的提交index
	aeReply := AppendEntriesReply{Term: term}
	for i := snextIndex; i < len(rf.log)+lastIncludedIndex; i++ {
		aeArgs.Entries = append(aeArgs.Entries, rf.log[i-lastIncludedIndex])
	}
	rf.sendAppendEntries(server, &aeArgs, &aeReply) // 并行发送保证Leader不会因为等待follower接收心跳耽误时间
	rf.mu.Lock()
	if aeReply.Success == false {
		if rf.currentTerm < aeReply.Term { // (Rules All 2)
			fmt.Printf("Server%d(%s %d) term过期，转变为Follower!\n", rf.me, roleMap(rf.role), term)
			rf.currentTerm = aeReply.Term
			rf.role = 0
			rf.votedFor = -1
			rf.persist() // 保存数据至persister
			rf.mu.Unlock()
		} else { // 若log不匹配则重发
			if rf.nextIndex[server] > 1 && rf.nextIndex[server] > rf.lastIncludedIndex {
				rf.nextIndex[server]--
				//rf.nextIndex[server] = 1
				rf.mu.Unlock()
				time.Sleep(hbInterval * time.Millisecond) // 间隔100ms再重发
				go rf.oneAppendEntries(server, term, commit)
			} else {
				rf.mu.Unlock()
			}
		}
	} else if aeReply.Success == true { // follower与leader的log一致		// TODO 不一定正确
		lastIndex, _ := rf.lastLog()
		rf.nextIndex[server] = lastIndex + 1
		rf.matchIndex[server] = lastIndex
		rf.mu.Unlock()
	}
}

// 向所有server发送AppendEntries RPC
func (rf *Raft) sendAE2All() {
	rf.mu.Lock()
	allPeers := len(rf.peers)
	fmt.Printf("Server%d(%s %d) 向所有其他服务器发送AppendEntries\n", rf.me, roleMap(rf.role), rf.currentTerm)
	fmt.Printf("Server%d log=[%v] commitIndex=%d lastApplied=%d\n", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
	rf.mu.Unlock()
	for i := 0; i < allPeers; i++ { // 向每个服务器发送心跳
		if i != rf.me {
			server := i
			rf.mu.Lock()
			//fmt.Printf("Server%d 向Server%d sendAppendEntries!\n", rf.me, server)
			term := rf.currentTerm // TODO 应该把心跳参数移到循环外面，保证每个follower收到一致的
			commit := rf.commitIndex
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.snapshot,
			}

			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				go rf.oneInstallSnapshot(i, args)
			} else {
				//fmt.Printf("Server%d(%s %d) 向Server%d发送AppendEntries\n", rf.me, roleMap(rf.role), rf.currentTerm, server)
				go rf.oneAppendEntries(server, term, commit)
			}
			rf.mu.Unlock()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false // true

	// Your code here (2B).
	if rf.killed() == false {
		rf.mu.Lock()
		if rf.role == 2 {
			isLeader = true
			lastIndex, _ := rf.lastLog()
			//if command != rf.log[len(rf.log)-1].Command {
			// logIndex重新正则化
			newLog := Log{lastIndex + 1, command, rf.currentTerm}
			rf.log = append(rf.log, newLog)
			rf.matchIndex[rf.me] = lastIndex + 1 // 以便过半提交log entry
			rf.nextIndex[rf.me] = lastIndex + 2  // lastIndex是此条log之前的index，+2才为此条log之后的index
			rf.persist()                         // 保存数据至persister
			//}
			index, _ = rf.lastLog()
			term = rf.currentTerm
		}
		rf.mu.Unlock()
	}
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
func (rf *Raft) ticker() {
	//time.Sleep(3 * time.Second)
	log.Printf("Server%d 启动!\n", rf.me)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case 0: // 如果当前rf为follower，则等待RPC请求，否则因超时进入选举
		Wait:
			for {
				rand.Seed(time.Now().UnixNano())                // 生成随机种子
				rvTimeout := rand.Intn(rvEnd-rvStart) + rvStart // 随机设置选举超时时间
				select {
				case <-rf.ch: // 如收到RPC消息则打印日志（会持续循环等待RPC，直到除非超时）
					rf.mu.Lock()
					//fmt.Printf("Server%d(%s %d) 收到ch：%v\n", rf.me, roleMap(role), rf.currentTerm, msg)
					rf.mu.Unlock()
				case <-time.After(time.Duration(rvTimeout) * time.Millisecond): // 未收到RPC消息，则超时转变为候选人
					rf.mu.Lock()
					fmt.Printf("Server%d(%s %d) 心跳超时,转变为候选人!\n", rf.me, roleMap(role), rf.currentTerm)
					rf.role = 1
					rf.currentTerm++
					rf.votedFor = -1
					rf.persist() // 保存数据至persister
					rf.mu.Unlock()
					break Wait // break外部的等待心跳for循环
				}
			}
		case 1: // 如果当前rf为candidate，则准备请求投票
		Vote:
			for {
				go rf.sendRV2All()                              // 保证执行请求投票的过程尽量不会影响计时器计时
				rand.Seed(time.Now().UnixNano())                // 生成随机种子
				rvTimeout := rand.Intn(rvEnd-rvStart) + rvStart // 随机设置选举超时时间
				select {                                        // TODO 此处选举超时可能存在问题，应该在发出投票请求前就开始计时
				case <-rf.ch: // 如果成功当选或收到appendEntries RPC 则跳出选举状态
					rf.mu.Lock()
					//fmt.Printf("Server%d(%s %d) 收到ch：%v\n", rf.me, roleMap(role), rf.currentTerm, msg)
					rf.mu.Unlock()
					break Vote // break外部的选举for循环
				case <-time.After(time.Duration(rvTimeout) * time.Millisecond): // 否则因选举超时而重新开始选举
					rf.mu.Lock()
					fmt.Printf("Server%d(%s %d) 选举超时!\n", rf.me, roleMap(role), rf.currentTerm)
					rf.currentTerm++
					rf.role = 1
					rf.votedFor = -1
					rf.persist() // 保存数据至persister
					rf.mu.Unlock()
				}
			}
		case 2: // 如果当前rf为leader，则定期发送心跳给所有服务器
			rf.sendAE2All()
			rf.mu.Lock()
			fmt.Printf("Server%d nextIndex=%v matchIndex=%v\n", rf.me, rf.nextIndex, rf.matchIndex)
			lastIndex, _ := rf.lastLog()
			for i := rf.commitIndex; i <= lastIndex; i++ { // 从commitIndex开始遍历到leader的log尾部
				if i <= 0 { // 用于跳过初始化日志
					continue
				}
				precommit := 0
				for sv := 0; sv < len(rf.peers); sv++ { // 遍历所有server
					if rf.matchIndex[sv] >= i { // 如果该server的log中有跟i相匹配的条目
						precommit++ // 则对条目i应用票数+1
					}
					if precommit > len(rf.peers)/2 { //} && rf.log[i-rf.lastIncludedIndex].Term == rf.currentTerm { // 当有大多数server已将条目i复制到自己的log中时
						rf.commitIndex = i // 置leader的commitIndex为i
						//go rf.sendApplyMsg()
						//rf.cd.Signal()
						fmt.Printf("******Server%d commitIndex=%d!\n", rf.me, rf.commitIndex)
						break
					}
				}
				if precommit <= len(rf.peers)/2 { // 若所有server中并没有大多数已将条目i复制，则跳出
					break
				}
			}
			go rf.sendApplyMsg(rf.commitIndex)
			rf.mu.Unlock()
			time.Sleep(hbInterval * time.Millisecond) // 间隔100ms发送一次心跳
		}
	}
	//rf.Kill()
	log.Printf("Server%d 退出!", rf.me)
}

// 向applyCh通道发送消息，表明已将当前server的log按commitIndex真正应用到状态机
func (rf *Raft) sendApplyMsg(commitIndex int) { // applyCh chan ApplyMsg
	//for {
	//	rf.cd.L.Lock()
	//	rf.cd.Wait()
	rf.mu.Lock()
	//commitIndex := rf.commitIndex
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	//lastApplied := rf.lastApplied
	rf.mu.Unlock()
	for rf.lastApplied < commitIndex { // 如果应用到状态机的log Index一直小于commitIndex则一直提交到等于为止
		rf.mu.Lock()
		rf.lastApplied++
		//lastIndex, _ := rf.lastLog()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command, // rf.log[rf.lastApplied].Command, len(rf.log)-lastIndex+rf.lastApplied-1
			CommandIndex: rf.lastApplied}
		fmt.Printf("Server%d command applied = %v\n", rf.me, msg.Command)
		rf.mu.Unlock()
		//go func() {
		rf.applyCh <- msg
		//}()
		//fmt.Printf("Server%d end ch\n", rf.me)
	}

	//	rf.cd.L.Unlock()
	//}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
// persister is a place for this server to save its persistent role,
// and also initially holds the most recent saved role, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = 0                                // 初始化为follower
	rf.votedFor = -1                           // 初始化投票指向为无
	rf.ch = make(chan string)                  // 创建通道
	rf.cd.L = new(sync.Mutex)                  // 初始化条件变量的锁
	rf.currentTerm = 1                         // 初始term置为1
	rf.log = append(rf.log, Log{0, "init", 0}) // 初始化时存放一个日志用于抵消数组下标从0开始的麻烦
	rf.commitIndex = 0                         // 初始提交日志index置为0
	rf.lastApplied = 0                         // 初始应用状态机index置为0
	rf.lastIncludedIndex = 0                   // 初始化快照的index
	rf.lastIncludedTerm = 0                    // 初始化快照的term

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log)) // 初始每个peer的nextIndex为1
		rf.matchIndex = append(rf.matchIndex, 0)         // 初始每个peer的matchIndex为0
	}

	// initialize from role persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg(rf.commitIndex)

	return rf
}
