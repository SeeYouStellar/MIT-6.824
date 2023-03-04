package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type TaskRequest struct {
	//MachineID 		int  // every worker has one MachineID
}

type TaskResponse struct {
	TaskNumber		int
	FilePath		string
	NReduce			int
	State 			int  // map--0 reduce--1 
}

type DoneRequest struct {
	Type 			int  // map--0 reduce--1
	TaskNumber  	int 
}

type DoneResponse struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
