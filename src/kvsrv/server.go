package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here
	kva map[string]string
	clientmap map[int64]int64
	oldvaluemap map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//kv.clientmap[args.Clerkid] = args.Reqid
	reply.Value = kv.kva[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientmap[args.Clerkid] != args.Reqid{
		kv.kva[args.Key] = args.Value
	}
	kv.clientmap[args.Clerkid] = args.Reqid
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientmap[args.Clerkid] == args.Reqid{
		reply.Value = kv.oldvaluemap[args.Clerkid]
		return
	}
	kv.clientmap[args.Clerkid] = args.Reqid
	value := kv.kva[args.Key]
	reply.Value = value
	kv.oldvaluemap[args.Clerkid] = value
	kv.kva[args.Key] = value + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kva = make(map[string]string)
	kv.clientmap = make(map[int64]int64)
	kv.oldvaluemap = make(map[int64]string)
	return kv
}
