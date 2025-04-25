package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	filenames []string // index is the map task id
	nReduce int
	availableMapTasks []int // a list of avaiable map task ids
	availableReduceTasks []int // a list of available reduce task ids
	doneMapTasks map[int]struct{}
	doneReduceTasks map[int]struct{}
	mapTaskTimestamp map[int]int64
	reduceTaskTimestamp map[int]int64
	workerIDs []int
	pleaseExit bool
	mu sync.Mutex
	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.WorkerID == 0 {
		// Generate a random int32 number.
		reply.WorkerID = rand.Int()
	} else {
		reply.WorkerID = args.WorkerID
	}
	if args.TaskID != -1 {
		if args.TaskType == 1 {
			delete(c.mapTaskTimestamp, args.TaskID)
			c.doneMapTasks[args.TaskID] = struct{}{}
		} else if args.TaskType == 2 {
			delete(c.reduceTaskTimestamp, args.TaskID)
			c.doneReduceTasks[args.TaskID] = struct{}{}
		}
		// fmt.Printf("YY_DEBUG done task args %+v, c %+v\n", args, c)
	}
	if len(c.doneReduceTasks) == c.nReduce {
		c.cond.Signal()
		c.pleaseExit = true
	}
	reply.PleaseExit = c.pleaseExit
	if len(c.doneMapTasks) == len(c.filenames) {
		if len(c.availableReduceTasks) == 0 {
			for k, v := range c.reduceTaskTimestamp {
				if time.Now().Unix() - v > 10 {
					fmt.Printf("long running task args %+v reply %+v", args, reply)
					delete(c.reduceTaskTimestamp, k)
					c.availableReduceTasks = append(c.availableReduceTasks, k)
				}
			}
		} 
		if len(c.availableReduceTasks) == 0 {
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 2
		reply.TaskID = c.availableReduceTasks[len(c.availableReduceTasks)-1]
		c.availableReduceTasks = c.availableReduceTasks[:len(c.availableReduceTasks)-1]
		c.reduceTaskTimestamp[reply.TaskID] = time.Now().Unix()
		reply.NReduce = c.nReduce
		// fmt.Printf("YY_DEBUG assign reduce task args %#v reply %#v c %#v\n", args, reply, c)
	} else {
		if len(c.availableMapTasks) == 0 {
			for k, v := range c.mapTaskTimestamp {
				if time.Now().Unix() - v > 10 {
					delete(c.mapTaskTimestamp, k)
					c.availableMapTasks = append(c.availableMapTasks, k)
				}
			}
		} 
		if len(c.availableMapTasks) == 0 {
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 1
		reply.TaskID = c.availableMapTasks[len(c.availableMapTasks)-1]
		reply.Filename = c.filenames[reply.TaskID]
		c.availableMapTasks = c.availableMapTasks[:len(c.availableMapTasks)-1]
		reply.NReduce = c.nReduce
		c.mapTaskTimestamp[reply.TaskID] = time.Now().Unix()
		// fmt.Printf("YY_DEBUG assign map task args %#v reply %#v c %#v\n", args, reply, c)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	ret := true
	c.cond = sync.NewCond(&c.mu)  // Create a new condition variable associated with the mutex     
	defer c.mu.Unlock()
	if len(c.doneReduceTasks) < c.nReduce {
		c.cond.Wait()
	}
	fmt.Println("coordinator: condition is being signaled")
	c.pleaseExit = true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := []int{}
	reduceTasks := []int{}
	for i := range files {
		mapTasks = append(mapTasks, i)
	}
	for i := range nReduce {
		reduceTasks = append(reduceTasks, i)
	}
	c := Coordinator{
		filenames: files,
		nReduce: nReduce,
		availableMapTasks: mapTasks,
		availableReduceTasks: reduceTasks,
		doneMapTasks: map[int]struct{}{},
		doneReduceTasks: map[int]struct{}{},
		mapTaskTimestamp: map[int]int64{},
		reduceTaskTimestamp: map[int]int64{},
		workerIDs: []int{},
		mu: sync.Mutex{},
	}
	// fmt.Printf("map files: %+v", files)
	// Your code here.


	c.server()
	go c.Done()
	return &c
}
