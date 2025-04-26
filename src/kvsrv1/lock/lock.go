package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	// "fmt"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	state string
	id string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, state: l, id: kvtest.RandValue(8), version: rpc.Tversion(0)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, _ := lk.ck.Get(lk.state)
		// fmt.Printf("acquire id %+v, value %+v, version %+v, err %+v\n", lk.id, value, version, err)
		if value == "" {
			err := lk.ck.Put(lk.state, lk.id, version)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				value, _, _ := lk.ck.Get(lk.state)
				if value == lk.id {
					return
				}
			}
			
		} 
		time.Sleep(100 *time.Millisecond)
		// fmt.Println("acquire end")		
	} 
}

func (lk *Lock) Release() {
	// Your code here
	for {
		_, version, _ := lk.ck.Get(lk.state)
		err := lk.ck.Put(lk.state, "", version)
	   // fmt.Printf("release id %+v, err %+v\n", lk.id, err)
	   if err == rpc.OK {
		   return
	   } 
	   if err == rpc.ErrMaybe {
		   value, _, _ := lk.ck.Get(lk.state)
		   if value == "" {
			   return
		   }
		   // lk.version = rpc.Tversion(uint32(lk.version)+1)
	   }
	}
	
}
