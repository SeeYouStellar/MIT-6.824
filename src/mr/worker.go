package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sort"
	"os"
	"io/ioutil"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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


	for {
		args := TaskRequest{}
		reply := TaskResponse{}
		ok := CallGetTask(&args, &reply)
		if ok {
			filepath := reply.FilePath
			maptasknumber := reply.Number
			nreduce := reply.NReduce
			file, err := os.Open(filepath)
			if err != nil {
				log.Fatalf("cannot open %v", filepath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filepath)
			}
			file.Close()
			intermediate := mapf(filepath, string(content)) // (k1,v1)->mapf->list(k2,v2)

			sort.Sort(ByKey(intermediate))
			
			i := 0
			// store the intermediate data into mr-X-Y
			
			for i < len(intermediate) {
				j := i + 1
				reducetasknumber := ihash(intermediate[i].Key) % nreduce
				oname := "mr-"+strconv.Itoa(maptasknumber)+"-"+strconv.Itoa(reducetasknumber)
				
				ofile, _ := os.Create(oname)
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[j].Value)
				}
				ofile.Close()
				i = j
			}
			break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallGetTask(args *TaskRequest, reply *TaskResponse) bool {

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	ret := true
	if ok {
		fmt.Printf("reply.FilePath:%s -- reply.Name:%d -- reply.NReduce:%d\n", reply.FilePath, reply.Number, reply.NReduce)
	} else {
		ret = false
		fmt.Printf("call failed!\n")
	}
	return ret
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "172.28.32.1"+":1234")
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
