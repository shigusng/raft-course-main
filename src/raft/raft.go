package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry 表示一个日志条目
type LogEntry struct {
	Term    int         // 日志条目被创建时的任期
	Command interface{} // 状态机命令
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int       // 当前任期
	votedFor     int       // 当前任期投票给的候选人，-1 表示未投票
	state        string    // 服务器状态："follower", "candidate", "leader"
	electionTime time.Time // 选举超时时间

	// 日志相关字段
	log         []LogEntry    // 日志条目
	commitIndex int           // 已知已提交的最高日志条目的索引
	lastApplied int           // 已应用到状态机的最高日志条目的索引
	nextIndex   []int         // 对于每个服务器，下一个要发送的日志条目索引
	matchIndex  []int         // 对于每个服务器，已知已复制的最高日志条目索引
	applyCh     chan ApplyMsg // 应用日志到状态机的通道
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == "leader"
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term         int // 候选人的任期号
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人的最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int  // 当前任期号，用于候选人更新自己的任期号
	VoteGranted bool // 是否投票给该候选人
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化回复
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 如果请求的任期号大于当前任期号，更新自己的任期号并转换为 follower 状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
		rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
	}

	// 检查候选人的任期号是否有效
	if args.Term < rf.currentTerm {
		return
	}

	// 日志比较：检查候选人的日志是否至少和自己的日志一样新
	isLogUpToDate := func() bool {
		// 自己的最后日志索引和任期
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		}

		// 候选人的最后日志任期大于自己的，说明候选人日志更新
		if args.LastLogTerm > lastLogTerm {
			return true
		}
		// 任期相等，检查日志索引
		if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			return true
		}
		return false
	}()

	// 如果还没有投票，并且候选人的日志足够新，就投票给该候选人
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // 领导者的任期号
	LeaderId     int        // 领导者的ID，以便于跟随者重定向请求
	PrevLogIndex int        // 新日志条目之前的日志条目的索引
	PrevLogTerm  int        // 新日志条目之前的日志条目的任期
	Entries      []LogEntry // 需要保存的日志条目（如果是心跳，则为空）
	LeaderCommit int        // 领导者的commitIndex
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // 当前任期号，用于领导者更新自己的任期号
	Success bool // 跟随者是否成功追加了日志条目
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化回复
	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. 检查任期号
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
		rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
	}

	if args.Term < rf.currentTerm {
		return // 领导者任期过期，拒绝请求
	}

	// 2. 转换为 follower 并重置选举超时
	rf.state = "follower"
	rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)

	// 3. 检查日志一致性：prevLogIndex 处的日志条目的任期号必须等于 prevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		return // 日志太短，没有 prevLogIndex
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return // 日志不一致，prevLogIndex 处的任期号不匹配
	}

	// 4. 日志匹配，接受新的日志条目
	// 删除冲突的条目（如果有），并追加新的条目
	nextIndex := args.PrevLogIndex + 1
	rf.log = append(rf.log[:nextIndex], args.Entries...)

	// 5. 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// 应用已提交的日志到状态机
		go rf.applyLogs()
	}

	reply.Success = true
}

// 应用已提交的日志到状态机
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

// 发送 AppendEntries RPC 的函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 发送心跳消息的函数
func (rf *Raft) sendHeartbeats() {
	// 心跳实际上是没有日志条目的 AppendEntries RPC
	rf.sendAppendEntriesToAll()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}

	// 构造新的日志条目
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	// 追加到自己的日志
	index := len(rf.log)
	rf.log = append(rf.log, logEntry)
	term := rf.currentTerm

	// 更新自己的matchIndex
	rf.matchIndex[rf.me] = index

	// 异步发送AppendEntries RPC给所有跟随者
	go func() {
		rf.sendAppendEntriesToAll()
	}()

	return index, term, true
}

// 发送AppendEntries RPC给所有跟随者
func (rf *Raft) sendAppendEntriesToAll() {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntriesToPeer(i, currentTerm)
		}
	}
}

// 发送AppendEntries RPC给特定的跟随者
func (rf *Raft) sendAppendEntriesToPeer(peerIndex int, term int) {
	rf.mu.Lock()
	if rf.state != "leader" || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	// 获取nextIndex和prevLogIndex
	nextIndex := rf.nextIndex[peerIndex]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0

	if prevLogIndex >= 0 {
		if prevLogIndex < len(rf.log) {
			prevLogTerm = rf.log[prevLogIndex].Term
		} else {
			// 处理日志索引越界
			rf.mu.Unlock()
			return
		}
	}

	// 获取要发送的日志条目
	entries := []LogEntry{}
	if nextIndex < len(rf.log) {
		entries = rf.log[nextIndex:]
	}

	// 构造AppendEntriesArgs
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	// 发送AppendEntries RPC
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(peerIndex, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != "leader" || rf.currentTerm != term {
			return
		}

		if reply.Success {
			// 更新nextIndex和matchIndex
			newMatchIndex := prevLogIndex + len(entries)
			rf.nextIndex[peerIndex] = newMatchIndex + 1
			rf.matchIndex[peerIndex] = newMatchIndex

			// 更新commitIndex
			rf.updateCommitIndex()
		} else {
			// 如果回复的任期号大于当前任期号，转换为follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = "follower"
				rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
			} else {
				// 日志不一致，减少nextIndex并重试
				rf.nextIndex[peerIndex] = max(1, rf.nextIndex[peerIndex]-1)
				// 异步重试
				go rf.sendAppendEntriesToPeer(peerIndex, term)
			}
		}
	}
}

// 更新commitIndex：找到多数节点已复制的日志索引
func (rf *Raft) updateCommitIndex() {
	if rf.state != "leader" {
		return
	}

	// 找到多数节点已复制的日志索引
	matchIndexes := make([]int, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			// 领导者自己的匹配索引是当前日志长度-1
			matchIndexes[i] = len(rf.log) - 1
		} else {
			matchIndexes[i] = rf.matchIndex[i]
		}
	}

	// 排序找到多数节点已复制的日志索引
	sort.Ints(matchIndexes)
	majorityIndex := matchIndexes[len(matchIndexes)/2]

	// 如果多数节点已复制，且日志条目属于当前任期，则提交
	// 同时，我们也需要处理之前任期的日志条目，如果它们被多数节点复制
	for n := rf.commitIndex + 1; n <= majorityIndex; n++ {
		if n < len(rf.log) {
			// 检查是否有多数节点已复制该日志条目
			count := 0
			for i := range rf.peers {
				if matchIndexes[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				// 该日志条目已被多数节点复制，可以提交
				rf.commitIndex = n
				// 应用已提交的日志到状态机
				go rf.applyLogs()
			}
		}
	}
}

// 辅助函数：返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) && rf.state != "leader" {
			// 开始选举
			rf.state = "candidate"
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
			currentTerm := rf.currentTerm
			me := rf.me
			rf.mu.Unlock()

			// 获取最后日志索引和任期
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := 0
			if lastLogIndex >= 0 {
				lastLogTerm = rf.log[lastLogIndex].Term
			}

			// 发送投票请求
			voteCount := 1     // 给自己投票
			receivedVotes := 1 // 已收到响应的节点数（包括自己）
			totalPeers := len(rf.peers)
			var mu sync.Mutex
			var cond = sync.NewCond(&mu)

			for i := range rf.peers {
				if i != rf.me {
					go func(server int) {
						args := &RequestVoteArgs{
							Term:         currentTerm,
							CandidateId:  me,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						}
						reply := &RequestVoteReply{}
						if rf.sendRequestVote(server, args, reply) {
							rf.mu.Lock()
							if rf.state == "candidate" && rf.currentTerm == currentTerm {
								if reply.VoteGranted {
									mu.Lock()
									voteCount++
									receivedVotes++
									mu.Unlock()
									cond.Signal()
								} else {
									mu.Lock()
									receivedVotes++
									mu.Unlock()
									cond.Signal()
								}
							} else if reply.Term > rf.currentTerm {
								// 如果回复的任期号大于当前任期号，更新自己的任期号并转换为 follower 状态
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.state = "follower"
								rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)
								mu.Lock()
								receivedVotes++
								mu.Unlock()
								cond.Signal()
							}
							rf.mu.Unlock()
						} else {
							// RPC 失败，增加 receivedVotes 计数，但不算投票
							mu.Lock()
							receivedVotes++
							mu.Unlock()
							cond.Signal()
						}
					}(i)
				}
			}

			// 等待足够多的投票结果或超时
			mu.Lock()
			// 等待直到获得大多数投票或收到所有响应
			for voteCount <= totalPeers/2 && receivedVotes < totalPeers {
				cond.Wait()
			}
			// 检查是否获得大多数投票
			if voteCount > totalPeers/2 {
				rf.mu.Lock()
				if rf.state == "candidate" && rf.currentTerm == currentTerm {
					rf.state = "leader"
					// 成为领导者后立即发送一次心跳
					rf.mu.Unlock()
					rf.sendHeartbeats()
				} else {
					rf.mu.Unlock()
				}
			}
			mu.Unlock()
		} else {
			// 如果是领导者，发送心跳消息
			if rf.state == "leader" {
				rf.mu.Unlock()
				rf.sendHeartbeats()
			} else {
				rf.mu.Unlock()
			}
		}

		// 领导者发送心跳的间隔应该比选举超时时间短
		if rf.state == "leader" {
			// 心跳间隔设置为 100ms
			time.Sleep(100 * time.Millisecond)
		} else {
			// 跟随者和候选人的超时检查间隔设置为 50-350ms 的随机值
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// 辅助函数：返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	rf.applyCh = applyCh

	// Your initialization code here (PartA, PartB, PartC).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "follower"
	rf.electionTime = time.Now().Add(time.Duration(50+rand.Int63()%300) * time.Millisecond)

	// 初始化日志相关字段
	// 日志索引从1开始，添加一个虚拟的空日志条目在索引0位置
	rf.log = make([]LogEntry, 1) // 索引0是虚拟条目
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// 初始化 nextIndex 和 matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) // 初始化为1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
