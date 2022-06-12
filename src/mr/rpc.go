package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type Task struct {
	TaskNumber    int
	Type          string
	Files         []string
	Allocated     bool
	AllocatedTime time.Time
	Done          bool
	Worker        string
	MMap          int
	NReduce       int
}

type RegisterWorkerArgs struct {
	Url string
}

type RegisterWorkerReply struct {
	Ok  bool
	Err error
}

type HeartBeatArgs struct {
	Url string
}

type HeartBeatReply struct {
	Ok  bool
	Err error
}

type RequestTaskArgs struct {
	Url string
}

type RequestTaskReply struct {
	Err  error
	Task Task
}

type TaskDoneArgs struct {
	Url        string
	TaskNumber int
	Type       string
}

type TaskDoneReply struct {
	Err error
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
