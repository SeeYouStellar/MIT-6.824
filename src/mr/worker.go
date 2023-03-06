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
	"time"
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
				
				// get data from rpc service  
				Y := reply.TaskNumber          // reduce task ID
				// nreduce := reply.ReduceTaskNum   // number of reduce task and mroutfile
				nmap := reply.MapTaskNum
				// decoder from mr-*-Y
				kva := []KeyValue{}
				for i:=0;i<nmap;i++ {
					intermediatefilepath := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(Y)
					intermediatefile, err := os.OpenFile(intermediatefilepath, os.O_RDONLY, 0777)
					if err != nil {
						log.Fatalf("cannot open reduceTask %v", intermediatefilepath)
						continue
					}
					dec := json.NewDecoder(intermediatefile)
					for {
						var kv []KeyValue    //official docs has trouble in this line
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv...)  // ... indicate add new slice
					}
				}
				sort.Sort(ByKey(kva))
				// accumulate every kv with the same key 
				// one reduce task operate some key 
				i := 0
				oname := "mr-out-"+strconv.Itoa(Y)
				ofile, _ := os.Create(oname)
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)    // per list(k2,v2)->per reducef->(k2,list(v2))
			
					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			
					i = j
				}
				ofile.Close()
			} else if reply.State == 0 {
				// mr in map phase
				
				// get data from rpc service  
				filepath := reply.FilePath  // file need to use mapf
				nreduce := reply.ReduceTaskNum    // number of reduce task and mroutfile
				X := reply.TaskNumber           // map task ID

				// use mapf 
				file, err := os.Open(filepath)
				if err != nil {
					log.Fatalf("cannot open %v", filepath)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filepath)
				}
				file.Close()
				kva := mapf(filepath, string(content)) // (k1,v1)->mapf->list(k2,v2)
				// fmt.Println(kva[1].Key, kva[1].Value)
				// use a buffer and write the buffer to file in nreduce times 
				// instead of write one kv in one time 
				// when worker broken at writing tmpfile, the tmpfile is disappear at the same time
				// it will prevent from writing twice at mr-X-Y when other worker do the same task
				buffer := make([][]KeyValue, nreduce)
				for _, kv := range kva {
					no := (ihash(kv.Key)) % nreduce
					buffer[no] = append(buffer[no], kv)
				}
				
				// write the buffer into tmpFile then rename it to mr-X-Y
				// X--mapnumber  Y--reducenumber
				
				for Y, kvWithKeyY := range buffer {
					MapOutFileName := "mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(Y)
					TmpFile, error := ioutil.TempFile("", "mr-map-*")
					if error != nil {
						log.Fatalf("cannot open TmpFile")
						continue
					}
					enc := json.NewEncoder(TmpFile)
					err := enc.Encode(kvWithKeyY)
					if err != nil {
						//fmt.Printf("write wrong!\n")
						log.Fatalf("write TmpFile wrong")
						continue
					}
					TmpFile.Close()
					os.Rename(TmpFile.Name(), MapOutFileName)
				}
			} else if reply.State == 3 {
				continue
			} else {
				// worker die
				// all reduce task have done 
				break
			}
			// notify master the task has been done
			for {
				doneargs := DoneRequest{Type:reply.State, TaskNumber:reply.TaskNumber}
				donereply := DoneResponse{}
				ok := CallTaskDone(&doneargs, &donereply)
				if ok {
					break
				}
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
		if reply.State != 3 {
			fmt.Printf("CallGetTask successfully!FilePath:%s;TaskNumber:%d;State:%d\n", reply.FilePath, reply.TaskNumber, reply.State)
		}
		
	} else {
		ret = false
		fmt.Printf("CallGetTask failed!\n")
	}
	return ret
}
func CallTaskDone(args *DoneRequest, reply *DoneResponse) bool {
	ok := call("Coordinator.TaskDone", &args, &reply)
	ret := true
	if ok {
		fmt.Printf("CallTaskDone successfully!State:%d;TaskNum:%d\n", args.Type, args.TaskNumber)
	} else {
		ret = false
		fmt.Printf("CallTaskDone failed!\n")
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
