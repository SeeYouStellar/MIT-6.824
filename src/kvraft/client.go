package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	clerkId    int
	lastSeq    int
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = int(nrand())
	ck.lastSeq = 0
	ck.lastLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		Seq:     ck.lastSeq,
	}
	reply := GetReply{}

	i := ck.lastLeader
	defer func() {
		ck.mu.Lock()
		ck.lastLeader = i
		ck.mu.Unlock()
	}()

	for {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		// fmt.Printf("clerk %d call cmd to server层 %d\n", ck.clerkId, i)
		// fmt.Println(args)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.lastSeq += 1
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		OpPA:    op,
		ClerkId: ck.clerkId,
		Seq:     ck.lastSeq,
	}
	reply := PutAppendReply{}

	i := ck.lastLeader
	defer func() {
		ck.mu.Lock()
		ck.lastLeader = i
		ck.mu.Unlock()
	}()

	for {
		// fmt.Printf("clerk %d send cmd to server层 %d\n", ck.clerkId, i)
		// fmt.Println(args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
