package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterWorkerRequest struct{}
type RegisterWorkerResponse struct {
	WorkerID int64
	NReduce  int64
}

const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
	TaskTypeNoMore = -1
	TaskTypeDone   = -2
)

type GetTaskRequest struct {
	WorkerID int64
}

type GetTaskResponse struct {
	TaskID              int64
	TaskType            int64
	MapTaskInputAddr    string
	ReudceTaskInputAddr map[int64]int64 // map task ID -> worker ID
}

type FinishTaskRequest struct {
	WorkerID int64 // 1:map/2:reduce
	TaskID   int64
	TaskType int
}

type FinishTaskResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock(workerID int64) string {
	return fmt.Sprintf("/var/tmp/824-mr-worker-%d", workerID)
}
