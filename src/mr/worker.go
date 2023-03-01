package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallGetMapTask()

	// CallMapTaskDone()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//


//my implement
func CallGetMapTask() {

	// declare an argument structure.
	args := TaskRequest{}

	// declare a reply structure.
	reply := TaskResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		fmt.Printf("reply.FilePath %s\n", reply.FilePath)
	} else {
		fmt.Printf("call failed!\n")
	}

	// file, err := os.Open(reply.FilePath)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", reply.FilePath)
	// }
	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", reply.FilePath)
	// }
	// file.Close()
	// intermediate := mapf(reply.FilePath, string(content)) // (k1,v1)->mapf->list(k2,v2)

	// oname := "mr-out-0"
	// ofile, _ := os.Create(oname)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
