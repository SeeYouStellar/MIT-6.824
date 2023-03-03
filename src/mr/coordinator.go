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
	MapTaskNum      int
	ReduceTaskNum   int
	state 			int // 0--start 1--map 2--reduce 
}

type Task struct {
	Number     		int // tasknumer
	FilePath 		string 
	RunningTime 		int // running time from worker get the task 
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	if c.state == 0 {
		// map task not finish
		maptask, ok:= <-c.MapTask
		if ok {
			reply.Number = maptask.Number
			reply.FilePath = maptask.FilePath
			reply.NReduce = c.ReduceTaskNum
			reply.State = c.state
		}
	} else if c.state == 1 {
		// all maptask finished
		reducetask, ok:= <-c.ReduceTask
		if ok {
			reply.Number = ReduceTask.Number
			reply.NReduce = c.ReduceTaskNum
			reply.State = c.state
		}
	} else {
		// mr finished 
		// master notify all worker to die
		reply.State = c.state
	}
	
	return nil
}

func (c *Coordinator) TaskDone(args *DoneRequest, reply *DoneResponse) error {
	if DoneRequest.Type == 0 {
		c.MapTaskDone <- true
	} else {
		c.ReduceTaskDone <- true
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
		MapTaskNum:len(files),
		ReduceTaskNum:nReduce,
		state:0,
	}

	// Your code here.
	// assign maptask
	for i, filepath := range files {
		c.MapTask <- Task{Number: i, FilePath: filepath, RunningTime: 0}
	}
	c.server()

	// Judge if or not all maptasks are done

	// assign reducetask
	// for i := 0; i < nReduce; i++ {

	// 	c.ReduceTasks <- ReduceTask{}
	// }
	return &c
}
