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
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"math/rand"
	"time"

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

	// persistent
	logs       []LogEntry
	nextIndex  []int
	matchIndex []int
	// in memory
	commitIndex  int
	lastApplied  int
	applyMsgchan chan ApplyMsg
}

// heartbeat timeout
var HeartBeatTimeout = 100 * time.Millisecond

// three states
const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

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

type AppendEntriesArgs struct {
	Term          int
	LeaderId      int
	PrevLogsIndex int
	PrevLogsTerm  int
	LeaderCommit  int
	Entries       []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).

	term = int(rf.currentTerm)
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

}

// example RequestVote RPC handler.
//
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	lastRfLogsIndex := len(rf.logs) - 1

	// operate state and term change

	if args.Term < rf.currentTerm {
		// fmt.Printf("server:%d [RequestVote]->server %d||args.Term < rf.currentTerm||args.Term:%d||rf.currentTerm:%d\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		// if candidate's term is bigger than follower/candidate, then
		// judge if candidate's logs is at least as up-to-date as follower/candidate's logs
		if args.LastLogsTerm > rf.logs[lastRfLogsIndex].Term || (args.LastLogsTerm == rf.logs[lastRfLogsIndex].Term && args.LastLogsIndex >= lastRfLogsIndex) {
			rf.state = STATE_FOLLOWER
			rf.voteFor = args.CandidateId
			rf.voteNum = 0
			rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond // prevent follower from starting a election
			rf.timer.Reset(rf.overTime)
			rf.currentTerm = args.Term
			reply.VoteGranted = true
			// fmt.Printf("server:%d [RequestVote]->server %d||return:%t\n", args.CandidateId, rf.me, reply.VoteGranted)
		} else {
			// fmt.Printf("server:%d [RequestVote]->server %d||return:%t||logs is not up-to-date\n", args.CandidateId, rf.me, reply.VoteGranted)
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.voteNum = 0
			rf.currentTerm = args.Term
			// 未投票不需要重置选举时间
			reply.Term = args.Term
		}
	} else {
		//  if has the same term, it should judge whether rf has voted for another candidate
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			// if the follower has not vote for another candidate, then
			// judge if candidate's logs is at least as up-to-date as follower/candidate's logs
			if args.LastLogsTerm > rf.logs[lastRfLogsIndex].Term || (args.LastLogsTerm == rf.logs[lastRfLogsIndex].Term && args.LastLogsIndex >= lastRfLogsIndex) {

				rf.voteFor = args.CandidateId
				rf.voteNum = 0
				rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond // prevent follower from starting a election
				rf.timer.Reset(rf.overTime)
				reply.VoteGranted = true

			} else {
				return
			}
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
	if args.Term < rf.currentTerm {
		// fmt.Printf("<outtime rpc>server:%d [sendRequestVote]->server:%d||args.Term < rf.currentTerm||args.term:%d||rf.currentTerm:%d\n", rf.me, server, args.Term, rf.currentTerm)
		return false
	}

	// operate the vote situation in this func instead of out of this func
	if reply.VoteGranted == true {
		rf.voteNum += 1
		if int(rf.voteNum) >= (len(rf.peers)/2)+1 {
			rf.state = STATE_LEADER
			rf.timer.Reset(0)
			for j := 0; j < len(rf.peers); j++ {
				rf.nextIndex[j] = len(rf.logs)
				rf.matchIndex[j] = 0
			}
			// fmt.Printf("term %d server %d become leader\n", rf.currentTerm, rf.me)
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.voteNum = 0

		} else {
			// 日志不够新 ->do nothing, 只要和有大多数比够新就行，和一个比不够新还不足以让这个candidate变为follower
			// 投给其他人 ->do nothing
			// fmt.Printf("server:%d [sendRequestVote]->server:%d||logs is not up-to-date||term:%d\n", rf.me, server, rf.currentTerm)
			// rf.state = STATE_FOLLOWER
			// rf.currentTerm = reply.Term
			// rf.voteFor = -1
			// rf.voteNum = 0
			// rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
			// rf.timer.Reset(rf.overTime)
		}
	}

	return true
}
func (rf *Raft) ApplyLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {

			rf.lastApplied += 1
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.logs[rf.lastApplied].Command,
			}
			rf.applyMsgchan <- applyMsg
			// fmt.Printf("server:%d [send applyMsg]||applyMsg.CommandIndex:%d||applyMsg.Command:%v\n", rf.me, applyMsg.CommandIndex, applyMsg.Command)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		// fmt.Printf("server %d AppendEntries false 领导者任期过期\n", rf.me)
		return
	}

	if (args.PrevLogsIndex < len(rf.logs) && rf.logs[args.PrevLogsIndex].Term != args.PrevLogsTerm) || args.PrevLogsIndex >= len(rf.logs) {
		// fmt.Printf("server %d AppendEntries false prev日志不匹配\n", rf.me)
		rf.state = STATE_FOLLOWER
		rf.voteNum = 0
		rf.voteFor = -1
		rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond // prevent follower from starting a election
		rf.timer.Reset(rf.overTime)
		rf.currentTerm = args.Term
		reply.Term = args.Term
		return
	}

	// judge if rf.logs have conflict with the new entry
	//
	i := 0
	for i = 1; i <= len(args.Entries); i++ {
		if len(rf.logs) > args.PrevLogsIndex+i && rf.logs[args.PrevLogsIndex+i].Term != args.Entries[i-1].Term {
			// delete the conflict entry and all that follow it
			rf.logs = rf.logs[:args.PrevLogsIndex+i] // simplify i to 1
			tmp := args.Entries[i-1:]
			rf.logs = append(rf.logs, tmp...)
			break
		} else if len(rf.logs) == args.PrevLogsIndex+i {
			tmp := args.Entries[i-1:]
			rf.logs = append(rf.logs, tmp...)
			break
		}
	}

	// fmt.Printf("server:%d||", rf.me)
	// for _, v := range rf.logs {
	// 	fmt.Printf("%v ", v.Command)
	// }
	// fmt.Printf("\n")
	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	// fmt.Printf("server %d AppendEntries true commitIndex:%d \n", rf.me, rf.commitIndex)
	// fmt.Printf("server %d log:\n", rf.me)
	// fmt.Println(rf.logs)
	rf.state = STATE_FOLLOWER
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.voteFor = -1
	rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
	rf.timer.Reset(rf.overTime)
	reply.Success = true
	// fmt.Printf("server:%d [AppendEntries]->server %d||return:%t||old len(logs):%d||new len(logs):%d||commitIndex:%d||lastApplied:%d\n", args.LeaderId, rf.me, true, oldlenlogs, len(rf.logs), rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// the same as sendRequestVote
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for ok == false {
		if rf.killed() == false {
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		} else {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// fmt.Printf("leader %d sendAppendEntries false 任期过期\n", rf.me)
		return false
	}

	if reply.Success == false {
		if reply.Term > rf.currentTerm {
			// fmt.Printf("leader %d sendAppendEntries -> server %d false 任期过期\n", rf.me, server)
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term

			rf.voteFor = -1
			rf.voteNum = 0
			rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
			rf.timer.Reset(rf.overTime)

		} else {
			// logs replica part: logs inconsistence
			// fmt.Printf("leader %d sendAppendEntries -> server %d false nextIndex[%d] %d->%d\n", rf.me, server, server, rf.nextIndex[server], rf.nextIndex[server]-1)
			rf.nextIndex[server] -= 1
		}
	} else {
		// fmt.Printf("leader %d sendAppendEntries -> server %d true", rf.me, server)
		rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		// fmt.Printf("update nextIndex[%d]=%d\n", server, rf.nextIndex[server])
		// fmt.Printf("update matchIndex[%d]=%d\n", server, rf.matchIndex[server])
		// update leader's commitIndex
		for i := len(rf.logs) - 1; i >= rf.commitIndex+1; i-- {
			cnt := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i && rf.logs[i].Term == rf.currentTerm {
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

	logentry := LogEntry{Term: rf.currentTerm, Command: command, Index: len(rf.logs)}
	rf.logs = append(rf.logs, logentry)

	term = rf.currentTerm
	index = logentry.Index
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	// fmt.Printf("client write log %d (index %d) to leader %d\n", logentry.Command, logentry.Index, rf.me)
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

	time.Sleep(5 * time.Millisecond) // wait tester to write "test(2A)"

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			// fmt.Printf("term %d||machine %d||state:%d\n", rf.currentTerm, rf.me, rf.state)
			switch rf.state {
			case STATE_FOLLOWER:
				rf.state = STATE_CANDIDATE
				// fmt.Printf("term %d, machine %d become a candidate\n", rf.currentTerm, rf.me)
				fallthrough // good idea
			case STATE_CANDIDATE:
				rf.currentTerm += 1
				rf.overTime = time.Duration(350+rand.Intn(150)) * time.Millisecond
				rf.timer.Reset(rf.overTime)

				rf.voteFor = rf.me
				rf.voteNum = 1

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
					if len(rf.logs) > 1 {
						args.LastLogsIndex = rf.logs[len(rf.logs)-1].Index
						args.LastLogsTerm = rf.logs[len(rf.logs)-1].Term
					}
					go rf.sendRequestVote(i, &args, &reply)
				}
			case STATE_LEADER:
				rf.timer.Reset(HeartBeatTimeout)
				for j := range rf.peers {
					if j == rf.me {
						continue
					}
					// commom heartbeat
					reply := AppendEntriesReply{}
					args := AppendEntriesArgs{
						Term:          rf.currentTerm,
						LeaderId:      rf.me,
						PrevLogsIndex: 0,
						PrevLogsTerm:  0,
						Entries:       nil,
						LeaderCommit:  rf.commitIndex,
					}
					if len(rf.logs) > 1 {

						if rf.nextIndex[j] >= 1 && rf.nextIndex[j] <= len(rf.logs) {
							args.PrevLogsIndex = rf.nextIndex[j] - 1
							args.PrevLogsTerm = rf.logs[args.PrevLogsIndex].Term
							if rf.nextIndex[j] < len(rf.logs) {
								args.Entries = rf.logs[rf.nextIndex[j]:]
							} else {
								args.Entries = nil
							}
						} else if rf.nextIndex[j] < 1 {
							args.PrevLogsIndex = 0
							args.PrevLogsTerm = 0
							args.Entries = rf.logs
						} else if rf.nextIndex[j] > len(rf.logs) {
							args.PrevLogsIndex = rf.logs[len(rf.logs)-1].Index
							args.PrevLogsTerm = rf.logs[len(rf.logs)-1].Term
							args.Entries = nil
						}
					}
					// fmt.Printf("term %d leader %d nextIndex[%d]: %d  matchIndex[%d]: %d\n", rf.currentTerm, rf.me, j, rf.nextIndex[j], j, rf.matchIndex[j])
					// fmt.Println(args.Entries)
					// fmt.Printf("term %d leader %d sendAppendEntries PrevLogsIndex: %d  PrevLogsTerm: %d leaderCommit: %d \n", rf.currentTerm, rf.me, args.PrevLogsIndex, args.PrevLogsTerm, args.LeaderCommit)
					go rf.sendAppendEntries(j, &args, &reply)
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

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyLogs()
	return rf
}
