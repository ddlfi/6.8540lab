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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequesetArgs struct {
}

type TaskReply struct {
	TaskType int
	FileName string
	TaskId   int
	NReduce  int
	NMap     int
	NoTaskFlag bool
}

type TaskFinishRequest struct {
	TaskId   int
	TaskType int
}

type TaskFinishReply struct {
}

//  b+0Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
