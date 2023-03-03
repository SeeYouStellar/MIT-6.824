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
	"encoding/json"
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

	// 1. worker should ask for task continuously(although worker has done a task) until all maptask has done 
	// 2. then worker ask for a reduce task continuously like above
	// worker use CallGetTask reply variable to get mr state
	
	for {
		args := TaskRequest{}
		reply := TaskResponse{}
		ok := CallGetTask(&args, &reply)
		// ok:if rpc call successfully or not
		if ok {
			if reply.State == 1 {
				// mr in reduce phase

			} else if reply.State == 0 {
				// mr in map phase
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
				
				// use a buffer and write the buffer to file in nreduce times 
				// instead of write one kv in one time 
				buffer := make([][]KeyValue, nreduce)
				for _, kv := range intermediate {
					no := (ihash(kv.Key)) % nreduce
					buffer[no].append(buffer[no], kv)
				}
				
				// write the buffer into tmpFile then rename it to mr-X-Y
				// X--mapnumber  Y--reducenumber
				X := reply.Number
				for Y, kva := range buffer {
					MapOutFileName := "mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(Y)
					TmpFile, error := ioutil.TempFile("", "mr-map-*")
					if error != nil {
						log.Fatalf("cannot open TmpFile")
					}
					enc := json.NewEncoder(TmpFile)
					err := enc.Encode(kva)
					if err != nil {
						//fmt.Printf("write wrong!\n")
						log.Fatalf("write TmpFile wrong")
					}
					TmpFile.Close()
					os.Rename(TmpFile.Name(), MapOutFileName)
				}
			} else {
				// worker die
				break
			}

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
