package raft

//export PATH=$PATH:/usr/local/go/bin
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"math/rand"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
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

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	state       int
	timer       *time.Timer
	overTime    time.Duration
	voteNum     int

	logs         []LogEntry
	nextIndex    []int
	matchIndex   []int
	commitIndex  int
	lastApplied  int
	applyMsgchan chan ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

// heartbeat timeout
var HeartBeatTimeout = 100 * time.Millisecond

// three states
const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

func (rf *Raft) LastLog() (int, int) {
	if len(rf.logs) <= 1 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	} else {
		return rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = (rf.state == STATE_LEADER)

	return term, isleader
}

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

	rf.persister.SaveRaftState(rf.encodeRaftState())
}
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		fmt.Printf("获取persist信息失败\n")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		// fmt.Printf("获取persist信息成功 server %d currentTerm %d voteFor %d logs ", rf.me, rf.currentTerm, rf.voteFor)
		// fmt.Println(logs)
		//注意:获取的过程中server变为leader然后client写了日志，此时日志就与获取时的不一样了，这里输出的只是持久化的日志
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Server%d start snapshot! index=%d\n", rf.me, index)
	lastIndex, _ := rf.LastLog()
	if index <= rf.lastIncludedIndex || index > rf.lastApplied || index > lastIndex { // 如果快照截止index不合理则拒绝执行
		fmt.Printf("server%d fail to snapshot\n", rf.me)
		fmt.Printf("lastIncludedIndex=%d lastApplied=%d lastIndex=%d\n", rf.lastIncludedIndex, rf.lastApplied, lastIndex)
		return
	}

	log := rf.logs[0:1] // 压缩log截至index，生成快照信息
	log[0].Index = index
	log[0].Term = rf.logs[index-rf.lastIncludedIndex].Term
	log[0].Command = "snapshot"
	log = append(log, rf.logs[index-rf.lastIncludedIndex+1:]...) // 保留index后的log
	rf.logs = log
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = log[0].Term

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot) // 持久化当前raft与快照状态
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	// 心跳功能
	rf.state = STATE_FOLLOWER
	rf.voteFor = args.LeaderId
	rf.voteNum = 0
	rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
	rf.timer.Reset(rf.overTime)
	rf.currentTerm = args.Term
	rf.persist()
	reply.Term = args.Term

	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}
	applyMsg := ApplyMsg{ // 封装apply报文
		SnapshotValid: true,      // 告诉应用状态机本次提交为快照
		Snapshot:      args.Data, // 提交快照与leader的一致
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm}

	// go func() { // 异步向通道发送apply报文
	// 	rf.applyMsgchan <- applyMsg
	// }()
	go rf.ApplySnapshotLog(applyMsg)
}
func (rf *Raft) ApplySnapshotLog(applyMsg ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyMsgchan <- applyMsg
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for ok == false {
		if rf.killed() == false {
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		} else {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term || rf.state != STATE_LEADER {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.voteNum = 0
		rf.state = STATE_FOLLOWER
		rf.persist()
		return false
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
	newCommitIndex := matchIndexSlice[len(rf.peers)/2]
	//不能提交不属于当前term的日志
	if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
		//如果commitIndex比自己实际的日志长度还大，这时需要减小
		lastIndex, _ := rf.LastLog()
		if newCommitIndex > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = newCommitIndex
		}
	}
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int
	CandidateId   int
	LastLogsIndex int
	LastLogsTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	LastIndex, LastTerm := rf.LastLog()

	// operate state and term change

	if args.Term < rf.currentTerm {
		// fmt.Printf("term %d server %d not vote for server, 任期不够新\n", rf.currentTerm, rf.me)
		return
	} else if args.Term > rf.currentTerm {
		// TODO 只要任期不够新就要变为follower，简化下面的分支
		// 候选者任期严格大于server的不需要判断是否已投票，因为投票的同时也同步了任期，说明它投给了任期小的candidate，那么直接一票否决
		if args.LastLogsTerm > LastTerm || (args.LastLogsTerm == LastTerm && args.LastLogsIndex >= LastIndex) {
			rf.voteFor = args.CandidateId
			rf.state = STATE_FOLLOWER
			rf.voteNum = 0
			rf.currentTerm = args.Term
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond // prevent follower from starting a election
			rf.timer.Reset(rf.overTime)
			rf.persist()
			// fmt.Printf("term %d server %d vote for server %d\n", rf.currentTerm, rf.me, args.CandidateId)
		} else {
			// fmt.Printf("term %d server %d not vote for server, 日志不够新\n", rf.currentTerm, rf.me)
			rf.voteFor = -1
			rf.state = STATE_FOLLOWER
			rf.voteNum = 0
			rf.currentTerm = args.Term
			reply.Term = args.Term
			rf.persist()
		}

	} else {
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			if args.LastLogsTerm > LastTerm || (args.LastLogsTerm == LastTerm && args.LastLogsIndex >= LastIndex) {
				rf.voteFor = args.CandidateId
				rf.voteNum = 0
				rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond // prevent follower from starting a election
				rf.timer.Reset(rf.overTime)
				reply.VoteGranted = true
				rf.persist()
				// fmt.Printf("term %d server %d vote for server %d\n", rf.currentTerm, rf.me, args.CandidateId)
			} else {
				// fmt.Printf("term %d server %d not vote for server, 日志不够新\n", rf.currentTerm, rf.me)
			}
		} else {
			// fmt.Printf("term %d server %d not vote for server, 投给了其他人\n", rf.currentTerm, rf.me)
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// candidate call RequestVote rpc via sendRequestVote function, so rf is the candidate

	// send at once
	// don't use lock when call rpc
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// sentRequestVote until success when meet the network problem or the server dead
	// there has another solution that use 10 times(or a static timeout) to call, if ok is still false, then return
	for ok == false {
		if rf.killed() == false {
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		} else {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rpc expiration: candidate call this func in args.term, and now it's term has been rf.currentTerm.
	// wasting so many time in the line286-288 will cause this problem
	if args.Term < rf.currentTerm || rf.state != STATE_CANDIDATE {
		fmt.Printf("term %d server %d get outdate rpc reply from server %d\n", rf.currentTerm, rf.me, server)
		return false
	}

	// operate the vote situation in this func instead of out of this func
	if reply.VoteGranted == true {
		rf.voteNum += 1
		// fmt.Printf("term %d server %d get vote from server %d\n", rf.currentTerm, rf.me, server)
		if rf.voteNum >= (len(rf.peers)/2)+1 {
			rf.state = STATE_LEADER
			rf.timer.Reset(0)

			for j := 0; j < len(rf.peers); j++ {
				LastIndex, _ := rf.LastLog()
				rf.nextIndex[j] = LastIndex + 1
				rf.matchIndex[j] = 0
			}
			// fmt.Printf("term %d server %d become leader\n", rf.currentTerm, rf.me)
			// fmt.Printf("------------------------------------------------------\n")
		}
	} else {
		if reply.Term > rf.currentTerm {
			// fmt.Printf("term %d server %d term is not up-to-date, become follower\n", rf.currentTerm, rf.me)
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.voteNum = 0
			rf.persist()
		}
		// 日志不够新 ->do nothing, 只要和有大多数比够新就行，和一个比不够新还不足以让这个candidate变为follower
		// 投给其他人 ->do nothing
	}

	return true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	LastIndex, _ := rf.LastLog()

	if args.Term < rf.currentTerm {
		// fmt.Printf("server %d AppendEntries false 领导者任期过期\n", rf.me)
		// fmt.Printf("------------------------------------------------------\n")
		return
	}

	// 当args.PrevLogIndex正好等于rf.lastIncludedIndex时，判断的是日志下标为0的日志条目
	// 所以生成快照时也要将这个日志条目的term和index调整为lastIncludedIndex和lastIncludedTerm，方便这里比较
	if args.PrevLogIndex <= LastIndex && rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		// fmt.Printf("server %d AppendEntries false prev日志不匹配\n", rf.me)
		// fmt.Printf("------------------------------------------------------\n")
		rf.state = STATE_FOLLOWER
		rf.voteNum = 0
		rf.voteFor = args.LeaderId
		rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
		rf.timer.Reset(rf.overTime)
		rf.currentTerm = args.Term
		rf.persist()
		reply.Term = args.Term
		// 优化后的日志回退策略
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
		for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- { // 如果都相同则ConflictIndex=1
			if rf.logs[i-rf.lastIncludedIndex].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}
	if args.PrevLogIndex >= LastIndex+1 {
		// fmt.Printf("server %d AppendEntries false prev日志不匹配\n", rf.me)
		// fmt.Printf("------------------------------------------------------\n")
		rf.state = STATE_FOLLOWER
		rf.voteNum = 0
		rf.voteFor = args.LeaderId
		rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
		rf.timer.Reset(rf.overTime)
		rf.currentTerm = args.Term
		rf.persist()
		reply.Term = args.Term
		// 优化后的日志回退策略
		reply.ConflictIndex = LastIndex + 1
		reply.ConflictTerm = -1
		return
	}
	// 日志冲突点检测
	if len(args.Entries)+args.PrevLogIndex <= LastIndex {
		for i := args.PrevLogIndex + 1; i <= len(args.Entries)+args.PrevLogIndex; i++ {
			if rf.logs[i-rf.lastIncludedIndex].Term != args.Entries[i-args.PrevLogIndex-1].Term {
				rf.logs = rf.logs[:args.PrevLogIndex+1-rf.lastIncludedIndex]
				rf.logs = append(rf.logs, args.Entries...)
				break
			}
		}
	} else {
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.lastIncludedIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}
	// i := 0
	// for i = 1; i <= len(args.Entries); i++ {
	// 	if len(rf.logs) > args.PrevLogIndex+i && rf.logs[args.PrevLogIndex+i].Term != args.Entries[i-1].Term {
	// 		rf.logs = rf.logs[:args.PrevLogIndex+i] // simplify i to 1
	// 		tmp := args.Entries[i-1:]
	// 		rf.logs = append(rf.logs, tmp...)
	// 		break
	// 	} else if len(rf.logs) == args.PrevLogIndex+i {
	// 		tmp := args.Entries[i-1:]
	// 		rf.logs = append(rf.logs, tmp...)
	// 		break
	// 	}
	// }

	// args.LeaderCommit会小于rf.commitIndex的情况并不会发生（等于的话也没要改变commitIndex了）
	// args.LeaderCommit会小于len(rf.logs)-1的情况是新的leader产生，而这个leader恰巧是之前宕机的，在其宕机期间，有很多日志复制到了各个server中
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > LastIndex {
			rf.commitIndex = LastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	// fmt.Printf("server %d AppendEntries true commitIndex:%d \n", rf.me, rf.commitIndex)
	// fmt.Printf("server %d log:\n", rf.me)
	// fmt.Println(rf.logs)
	// fmt.Printf("------------------------------------------------------\n")
	rf.state = STATE_FOLLOWER
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.voteFor = args.LeaderId
	rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
	rf.timer.Reset(rf.overTime)
	rf.persist()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// the same as sendRequestVote
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// 防止由于网络问题导致的rpc请求失败
	for !ok {
		if !rf.killed() {
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		} else {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LastIndex, _ := rf.LastLog()
	if args.Term < rf.currentTerm || rf.state != STATE_LEADER {
		// fmt.Printf("leader %d sendAppendEntries false 任期过期\n", rf.me)
		// fmt.Printf("------------------------------------------------------\n")
		return false
	}

	if reply.Success == false {
		if reply.Term > rf.currentTerm {
			// fmt.Printf("leader %d sendAppendEntries -> server %d false 任期过期\n", rf.me, server)
			// fmt.Printf("------------------------------------------------------\n")
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term

			rf.voteFor = -1
			rf.voteNum = 0
			rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
			rf.timer.Reset(rf.overTime)
			rf.persist()
		} else {
			// fmt.Printf("leader %d sendAppendEntries -> server %d false nextIndex[%d] %d->%d\n", rf.me, server, server, rf.nextIndex[server], rf.nextIndex[server]-1)
			// fmt.Printf("------------------------------------------------------\n")
			// 未优化的日志回退策略
			// rf.nextIndex[server] -= 1

			// 优化后的日志回退策略
			if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term不同
				conflictIndex := -1
				for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
					if rf.logs[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
						conflictIndex = i
						break
					}
				}
				if conflictIndex != -1 {
					// 如果leader.log找到了Term为conflictTerm的日志，则下一次从leader.log中conflictTerm的最后一个log的下一个位置开始同步日志。
					rf.nextIndex[server] = conflictIndex + 1
				} else {
					// 如果leader.log找不到Term为conflictTerm的日志，则下一次从follower.log中conflictTerm的第一个log的位置开始同步日志。
					rf.nextIndex[server] = reply.ConflictIndex
				}
			} else { // follower的prevLogIndex位置没有日志
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	} else {
		// fmt.Printf("leader %d sendAppendEntries -> server %d true", rf.me, server)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// fmt.Printf("update nextIndex[%d]=%d\n", server, rf.nextIndex[server])
		// fmt.Printf("update matchIndex[%d]=%d\n", server, rf.matchIndex[server])
		// 更新leader的commitIndex
		for i := LastIndex; i >= rf.commitIndex+1; i-- {
			cnt := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i && rf.logs[i-rf.lastIncludedIndex].Term == rf.currentTerm {
					cnt += 1
				}
			}
			if cnt >= (len(rf.peers)/2)+1 {
				rf.commitIndex = i
				break
			}
		}

		// fmt.Printf("update leader CommitIndex=%d\n", rf.commitIndex)
		// fmt.Printf("leader log:\n")
		// fmt.Println(rf.logs)
		// fmt.Printf("------------------------------------------------------\n")
	}
	return true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != STATE_LEADER {
		return index, term, false
	}

	LastIndex, _ := rf.LastLog()
	log := LogEntry{Term: rf.currentTerm, Command: command, Index: LastIndex + 1} // 不要用len(rf.logs)来代替Index
	rf.logs = append(rf.logs, log)
	rf.persist()

	term = log.Term
	index = log.Index
	rf.matchIndex[rf.me] = log.Index
	rf.nextIndex[rf.me] = log.Index + 1

	// fmt.Printf("client write log %d (index %d) to leader %d\n", log.Command, log.Index, rf.me)
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

func (rf *Raft) ApplyLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {

			rf.lastApplied += 1
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command,
			}
			rf.applyMsgchan <- applyMsg
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	time.Sleep(5 * time.Millisecond) // wait tester to write "test(2A)"

	for rf.killed() == false {

		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case STATE_FOLLOWER:
				rf.state = STATE_CANDIDATE
				fallthrough // good idea
			case STATE_CANDIDATE:
				rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
				rf.timer.Reset(rf.overTime)
				rf.currentTerm += 1
				rf.voteFor = rf.me
				rf.voteNum = 1
				rf.persist()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					reply := RequestVoteReply{}

					args := RequestVoteArgs{
						Term:          rf.currentTerm,
						CandidateId:   rf.me,
						LastLogsIndex: 0,
						LastLogsTerm:  0,
					}
					LastIndex, LastTerm := rf.LastLog()

					args.LastLogsIndex = LastIndex
					args.LastLogsTerm = LastTerm

					go rf.sendRequestVote(i, &args, &reply)
				}
			case STATE_LEADER:
				rf.timer.Reset(HeartBeatTimeout)
				for j := range rf.peers {
					if j == rf.me {
						continue
					}
					if rf.nextIndex[j] > rf.lastIncludedIndex {
						// 发日志增量
						LastIndex, LastTerm := rf.LastLog()
						reply := AppendEntriesReply{}
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}
						if len(rf.logs) > 1 {
							if rf.nextIndex[j] >= 1 && rf.nextIndex[j] <= LastIndex+1 {
								args.PrevLogIndex = rf.nextIndex[j] - 1
								args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
								if rf.nextIndex[j] < LastIndex+1 {
									args.Entries = rf.logs[rf.nextIndex[j]-rf.lastIncludedIndex:]
								} else {
									args.Entries = nil
								}
							} else if rf.nextIndex[j] < 1 {
								args.PrevLogIndex = 0
								args.PrevLogTerm = 0
								args.Entries = rf.logs
							} else if rf.nextIndex[j] > LastIndex+1 {
								args.PrevLogIndex = LastIndex
								args.PrevLogTerm = LastTerm
								args.Entries = nil
							}
						}
						// fmt.Printf("term %d leader %d nextIndex[%d]: %d  matchIndex[%d]: %d\n", rf.currentTerm, rf.me, j, rf.nextIndex[j], j, rf.matchIndex[j])
						// fmt.Println(args.Entries)
						// fmt.Printf("term %d leader %d sendAppendEntries PrevLogIndex: %d  PrevLogTerm: %d leaderCommit: %d \n", rf.currentTerm, rf.me, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
						// fmt.Printf("------------------------------------------------------\n")
						go rf.sendAppendEntries(j, &args, &reply)
					} else {
						// 发快照
					}

				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
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

	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = STATE_FOLLOWER
	rf.overTime = time.Duration(200+rand.Intn(250)) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyMsgchan = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyLogs()
	return rf
}
