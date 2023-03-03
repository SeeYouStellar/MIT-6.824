package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	MapTask         chan Task
	ReduceTask      chan Task
	MapTaskDone     chan bool
	ReduceTaskDone  chan bool
	MapWorkerNum    int
	ReduceWorkerNum int
	MapTaskNum      int
	ReduceTaskNum   int
	state 			int // 0--start 1--map 2--reduce 
}

type Task struct {
	Number     int
	FilePath string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	if c.state == 0 {
		maptask, ok:= <-c.MapTask
		if ok {
			reply.Number = maptask.Number
			reply.FilePath = maptask.FilePath
			reply.NReduce = c.ReduceTaskNum
			reply.State = c.state
		}
	} else if c.state == 1 {
		// all maptask finished
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
	ret := true

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTask:make(chan Task, len(files)),
		ReduceTask:make(chan Task, nReduce),
		MapTaskDone:make(chan bool, len(files)),
		ReduceTaskDone:make(chan bool, nReduce),
		MapWorkerNum:len(files)-1,
		ReduceWorkerNum:nReduce,
		MapTaskNum:len(files),
		ReduceTaskNum:nReduce,
		state:0,
	}

	// Your code here.
	// assign maptask
	for i, filepath := range files {
		c.MapTask <- Task{Number: i, FilePath: filepath}
	}
	c.server()
	// Judge if or not all maptasks are done

	// assign reducetask
	// for i := 0; i < nReduce; i++ {

	// 	c.ReduceTasks <- ReduceTask{}
	// }
	return &c
}
