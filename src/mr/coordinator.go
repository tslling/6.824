package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	mapTaskTimeoutSecond    = 10
	reduceTaskTimeoutSecond = 10
)

type Task struct {
	id               int64
	workerID         int64
	mapTaskInputFile string
	done             bool
	cancelTimeout    *time.Timer
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int64
	nextWorkerID int64

	muMap              sync.Mutex
	mapTasks           map[int64]*Task
	unassignedMapTasks chan *Task

	finishedMapTaskCount int32
	maxMapTaskCount      int32

	muReduce              sync.Mutex
	reduceTasks           map[int64]*Task
	unassignedReduceTasks chan *Task

	finishedReduceTaskCount int32

	doneFlag int32

	// task id -> worker id
	mapTask2worker sync.Map
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

func (c *Coordinator) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	reply.WorkerID = atomic.AddInt64(&c.nextWorkerID, 1)
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	select {
	case t := <-c.unassignedMapTasks:
		t.workerID = args.WorkerID
		mapTaskID := t.id
		cancel := time.AfterFunc(time.Second*mapTaskTimeoutSecond, func() {
			c.muMap.Lock()
			if t, exist := c.mapTasks[mapTaskID]; exist && !t.done {
				delete(c.mapTasks, mapTaskID)
				c.unassignedMapTasks <- t
			}
			c.muMap.Unlock()
		})
		t.cancelTimeout = cancel

		c.muMap.Lock()
		c.mapTasks[mapTaskID] = t
		c.muMap.Unlock()
		*reply = GetTaskResponse{
			TaskID:           mapTaskID,
			TaskType:         TaskTypeMap,
			MapTaskInputAddr: fmt.Sprintf(t.mapTaskInputFile),
		}
		return nil
	default:
	}

	select {
	case t := <-c.unassignedReduceTasks:
		t.workerID = args.WorkerID
		reduceTaskID := t.id
		cancel := time.AfterFunc(time.Second*reduceTaskTimeoutSecond, func() {
			c.muReduce.Lock()
			if t, exist := c.reduceTasks[reduceTaskID]; exist && !t.done {
				delete(c.mapTasks, reduceTaskID)
				c.unassignedReduceTasks <- t
			}
			c.muReduce.Unlock()
		})
		t.cancelTimeout = cancel

		c.muReduce.Lock()
		c.reduceTasks[reduceTaskID] = t
		c.muReduce.Unlock()

		task2worker := make(map[int64]int64)
		c.mapTask2worker.Range(func(key, value interface{}) bool {
			taskID := key.(int64)
			workerID := value.(int64)
			task2worker[taskID] = workerID
			return true
		})
		*reply = GetTaskResponse{
			TaskID:              reduceTaskID,
			TaskType:            TaskTypeReduce,
			ReudceTaskInputAddr: task2worker,
		}
		return nil
	default:
	}
	if atomic.LoadInt32(&c.doneFlag) == 1 {
		*reply = GetTaskResponse{
			TaskType: TaskTypeDone,
		}
	} else {
		*reply = GetTaskResponse{
			TaskType: TaskTypeNoMore,
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskRequest, reply *FinishTaskResponse) error {
	debugLog("finish task request recieved:%+v\n", args)
	switch args.TaskType {
	case TaskTypeMap:
		c.muMap.Lock()
		if task, exist := c.mapTasks[args.TaskID]; !exist {
			c.muMap.Unlock()
			return fmt.Errorf("unknown task id(%d)", args.TaskID)
		} else {
			task.cancelTimeout.Stop()
			task.done = true
		}
		c.muMap.Unlock()
		c.mapTask2worker.Store(args.TaskID, args.WorkerID)
		if atomic.AddInt32(&c.finishedMapTaskCount, 1) == c.maxMapTaskCount {
			debugLog("all map finished")
			var i int64 = 0
			for i = 0; i < c.nReduce; i++ {
				c.unassignedReduceTasks <- &Task{
					id: i,
				}
			}
		}
	case TaskTypeReduce:
		c.muReduce.Lock()
		if task, exist := c.reduceTasks[args.TaskID]; !exist {
			c.muReduce.Unlock()
			return fmt.Errorf("unknown task id(%d)", args.TaskID)
		} else {
			task.cancelTimeout.Stop()
			task.done = true
		}
		c.muReduce.Unlock()
		if atomic.AddInt32(&c.finishedReduceTaskCount, 1) == int32(c.nReduce) {
			atomic.StoreInt32(&c.doneFlag, 1)
		}
	default:
		return fmt.Errorf("unknown task type:%d", args.TaskType)
	}
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
	return atomic.LoadInt32(&c.doneFlag) == 1
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:              int64(nReduce),
		mapTasks:             make(map[int64]*Task, len(files)),
		unassignedMapTasks:   make(chan *Task, len(files)),
		finishedMapTaskCount: 0,
		maxMapTaskCount:      int32(len(files)),

		reduceTasks:             make(map[int64]*Task, nReduce),
		unassignedReduceTasks:   make(chan *Task, nReduce),
		finishedReduceTaskCount: 0,
	}

	for i, file := range files {
		c.unassignedMapTasks <- &Task{
			id:               int64(i),
			mapTaskInputFile: file,
		}
	}

	c.server()
	return &c
}

func callWorker(workerID int64, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := workerSock(workerID)
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

var debug = false

func debugLog(format string, args ...interface{}) {
	if debug {
		log.Printf(format, args)
	}
}
