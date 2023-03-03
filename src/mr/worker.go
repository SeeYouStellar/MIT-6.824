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
				Y := reply.Number          // reduce task ID
				nreduce := reply.NReduce   // number of reduce task and mroutfile
				 
				// decoder from mr-*-Y
				kva := ByKey{}
				for i:=0;i<nreduce;i++ {
					intermediatefilepath := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(Y)
					dec := json.NewDecoder(intermediatefilepath)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
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
				nreduce := reply.NReduce    // number of reduce task and mroutfile
				X := reply.Number           // map task ID

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
				
				// use a buffer and write the buffer to file in nreduce times 
				// instead of write one kv in one time 
				buffer := make([][]KeyValue, nreduce)
				for _, kv := range kva {
					no := (ihash(kv.Key)) % nreduce
					buffer[no].append(buffer[no], kv)
				}
				
				// write the buffer into tmpFile then rename it to mr-X-Y
				// X--mapnumber  Y--reducenumber
				
				for Y, kvWithKeyY := range buffer {
					MapOutFileName := "mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(Y)
					TmpFile, error := ioutil.TempFile("", "mr-map-*")
					if error != nil {
						log.Fatalf("cannot open TmpFile")
					}
					enc := json.NewEncoder(TmpFile)
					err := enc.Encode(kvWithKeyY)
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
		time.Sleep()
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
