package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	mem map[string]Value
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mem: map[string]Value{},
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, ok := kv.mem[key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Err = rpc.OK
	reply.Value = value.Value
	reply.Version = value.Version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	version := args.Version
	kvPair, ok := kv.mem[key]
	if !ok {
		if version != rpc.Tversion(0) {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.mem[key] = Value{
			Value: value,
			Version: 1,
		}
		reply.Err = rpc.OK
	} else {
		if version == kvPair.Version {
			kv.mem[key] = Value{
				Value: value,
				Version: rpc.Tversion(uint64(version)+1),
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
		
	}
	
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
