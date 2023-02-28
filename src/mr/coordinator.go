package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"



type Coordinator struct {
	// Your definitions here.
	MapTasks chan MapTask
	ReduceTasks chan ReduceTask
	MapWorkerNum int
	ReduceWorkerNum int
	MapTasksDone chan MapTask
	ReduceTasksDone chan ReduceTask
}

type MapTask struct {
	FilePath string
}
type ReduceTask struct {
	FilePaths []string
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// my implement
func (c *Coordinator) GetMapTask(args *TaskRequest, reply *MapTaskResponse) error {
	maptask := <-c.MapTasks
	c.MapWorkerNum += 1

	reply.FilePath = maptask.FilePath
	return nil
}
func (c *Coordinator) GetReduceTask(args *TaskRequest, reply *ReduceTaskResponse) error {
	reducetask = <-c.ReduceTasks
	c.ReduceWorkerNum += 1
	
	reply.FilePaths = reducetask.FilePaths
	return nil
}

func (c * Coordinator) MapTaskDone(args , reply *MapTaskDoneResponse) error {
	
	return nil
}

func (c * Coordinator) ReduceTaskDone(args , reply *ReduceTaskDoneResponse) error {
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
	c := Coordinator{}

	// Your code here.
	// assign maptask
	for _,filepath := range files {
		c.MapTasks <- MapTask{filepath}
	}
	c.server()
	// Judge if or not all maptasks are done

	// assign reducetask
	for i:=0;i<nReduce;i++ {

		c.ReduceTasks <- ReduceTask{}
	}
	return &c
}
