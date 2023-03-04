package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTask			map[int]*Task
	ReduceTask 		map[int]*Task
	MapTaskNum      int
	ReduceTaskNum   int
	State 			int // 0--start 1--all map done 2--all reduce done 
	Mu 				sync.Mutex
}

type Task struct {
	FilePath 		string 
	RunningTime 	int // running time from worker get the task 
	State			int // 0--undo 1--in progress 2--done
	// MachineID		int // the same worker write at the same local file when do different task 
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	c.Mu.Lock()
	if c.State == 0 {
		// map task not finish
		for i, maptask := range c.MapTask {
			if maptask.State == 0 {
				reply.TaskNumber = i
				reply.FilePath = maptask.FilePath
				reply.ReduceTaskNum = c.ReduceTaskNum
				reply.State = c.State
				maptask.State = 1
				break
			} 
		}
	} else if c.State == 1 {
		// all maptask finished
		for i, reducetask := range c.ReduceTask {
			if reducetask.State == 0 {
				reply.TaskNumber = i
				reply.ReduceTaskNum = c.ReduceTaskNum
				reply.State = c.State
				reply.MapTaskNum = c.MapTaskNum
				reducetask.State = 1
				break
			} 
		}
	} else {
		// mr finished 
		// master notify all worker to die
		reply.State = c.State
	}
	c.Mu.Unlock()
	return nil
}

func (c *Coordinator) TaskDone(args *DoneRequest, reply *DoneResponse) error {
	c.Mu.Lock()
	no := args.TaskNumber
	if args.Type == 0 {
		c.MapTask[no].State = 2
	} else if args.Type == 1 {
		c.ReduceTask[no].State = 2
	}
	c.Mu.Unlock()
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
	ret := false
	// Your code here.
	c.Mu.Lock()
	if c.State == 2 {
		ret = true
	}
	c.Mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTask:make(map[int]*Task),
		ReduceTask:make(map[int]*Task),
		MapTaskNum:len(files),
		ReduceTaskNum:nReduce,
		State:0,
		Mu:sync.Mutex{},
	}

	// Your code here.
	// assign maptask
	for i, filepath := range files {
		c.MapTask[i] = &Task{FilePath:filepath, RunningTime:0, State:0}
	}
	// assign reducetask
	for i := 0; i < nReduce; i++ {
		c.ReduceTask[i] = &Task{RunningTime:0, State:0}
	}

	c.server()

	// Judge if or not all maptasks are done
	for {
		c.Mu.Lock()
		if c.State == 2 {
			c.Mu.Unlock()
			break
		}
		i := 0
		for ;i<len(files);i++ {
			if c.MapTask[i].State == 1 {
				c.MapTask[i].RunningTime = c.MapTask[i].RunningTime+1
				if c.MapTask[i].RunningTime >= 10 {
					c.MapTask[i].State = 0
					c.MapTask[i].RunningTime = 0
				}
				break
			} else if c.MapTask[i].State == 0 {
				break
			}
		}
		if i == len(files) {
			c.State = 1
		}
		i = 0
		for ;i<len(files);i++ {
			if c.ReduceTask[i].State == 1 {
				c.ReduceTask[i].RunningTime = c.ReduceTask[i].RunningTime+1
				if c.ReduceTask[i].RunningTime >= 10 {
					c.ReduceTask[i].State = 0
					c.ReduceTask[i].RunningTime = 0
				}
				break
			} else if c.ReduceTask[i].State == 0 {
				break
			}
		}
		if i == nReduce {
			c.State = 2
		}
		c.Mu.Unlock()
		time.Sleep(time.Second)
	}

	return &c
}
