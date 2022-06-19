package mr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const debugWord = "ACT"

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

	fs := NewLocalFileSystem()
	regResult, err := RegisterWorker()
	if err != nil {
		log.Print(err.Error())
		return
	}
	workerID, nReduce := regResult.WorkerID, regResult.NReduce

	var (
		heartBeatTicker = time.NewTicker(5 * time.Second)
	)

	for {
		select {
		case <-heartBeatTicker.C:
			task, err := GetTask(workerID)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			debugLog("get task:%+v", task)
			taskID := task.TaskID
			if taskID == TaskTypeNoMore {
				// no more tasks now
				continue
			} else if taskID == TaskTypeDone {
				// done
				debugLog("all done")
				return
			}
			if task.TaskType == TaskTypeMap {
				go func() {
					err := doMap(workerID, taskID, nReduce, task.MapTaskInputAddr, mapf)
					if err != nil {
						log.Printf("do map error:%s", err.Error())
						return
					}
					err = FinishTask(workerID, taskID, TaskTypeMap)
					if err != nil {
						log.Printf("finish map error:%s", err.Error())
					}
				}()
			}
			if task.TaskType == TaskTypeReduce {
				go func() {
					// collect input
					allKV := make(map[string][]string)
					for mapTaskID, mapWorkerID := range task.ReudceTaskInputAddr {
						bs, err := fs.Read(fmt.Sprintf("mr-%d-%d", mapTaskID, taskID))
						if err != nil {
							log.Printf("read file error(map task id:%d, worker id:%d) :%s", mapTaskID, mapWorkerID, err.Error())
							// TODO: report to coordinator
							return
						}
						var kvs []KeyValue
						err = json.Unmarshal(bs, &kvs)
						if err != nil {
							log.Printf("unmarshal kv error(map task id:%d, worker id:%d) :%s, got:%s", mapTaskID, mapWorkerID, err.Error(), string(bs))
							// TODO: report to coordinator
							return
						}
						for _, kv := range kvs {
							if kv.Key == debugWord {
								log.Printf("reduce %s, key:%s, value:%s, reduce task id:%d, map task id:%d, map worker id:%d\n", debugWord, kv.Key, kv.Value, taskID, mapTaskID, mapWorkerID)
							}
							allKV[kv.Key] = append(allKV[kv.Key], kv.Value)
						}
					}
					buf := &bytes.Buffer{}
					for k, vs := range allKV {
						output := reducef(k, vs)
						buf.WriteString(fmt.Sprintf("%v %v\n", k, output))
					}
					err = fs.Write(fmt.Sprintf("mr-out-%d", taskID), buf.Bytes())
					if err != nil {
						log.Printf("write reduce result error:%s", err.Error())
					}
					err = FinishTask(workerID, taskID, TaskTypeReduce)
					if err != nil {
						log.Printf("finish task error:%s", err.Error())
					}
				}()
			}
		}
	}

}

func doMap(workerID, taskID, nReduce int64, taskInputAddr string, mapf func(string, string) []KeyValue) error {
	fs := NewLocalFileSystem()
	data, err := fs.Read(taskInputAddr)
	if err != nil {
		return fmt.Errorf("read task(id:%d, addr:%s) data error:%s\n", taskID, taskInputAddr, err.Error())
	}
	kvs := mapf(taskInputAddr, string(data))
	// write map result
	tmp := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % int(nReduce)
		if kv.Key == debugWord {
			debugLog("map word %s in map task %d, reduce task %d\n", debugWord, taskID, idx)
		}
		tmp[idx] = append(tmp[idx], kv)
	}
	for idx, t := range tmp {
		bs, _ := json.Marshal(t)
		f, err := ioutil.TempFile("", "mr-")
		if err != nil {
			return fmt.Errorf("write map result error: %s", err.Error())
		}
		tempFileName := f.Name()
		_, err = f.Write(bs)
		if err != nil {
			return fmt.Errorf("write temp file erro:%s", err.Error())
		}
		err = f.Close()
		if err != nil {
			return fmt.Errorf("close temp file erro:%s", err.Error())
		}
		err = os.Rename(tempFileName, fmt.Sprintf("./mr-%d-%d", taskID, idx))
		if err != nil {
			return fmt.Errorf("rename temp file erro:%s", err.Error())
		}
	}
	return nil
}

func RegisterWorker() (*RegisterWorkerResponse, error) {
	var (
		req  = RegisterWorkerRequest{}
		resp = RegisterWorkerResponse{}
	)
	ok := call("Coordinator.RegisterWorker", &req, &resp)
	if !ok {
		return nil, errors.New("register worker error")
	}
	return &resp, nil
}

func GetTask(workerID int64) (*GetTaskResponse, error) {
	req := GetTaskRequest{
		WorkerID: workerID,
	}
	resp := GetTaskResponse{}

	ok := call("Coordinator.GetTask", &req, &resp)
	if !ok {
		return nil, errors.New("get task fail")
	}
	return &resp, nil
}

func FinishTask(workerID, taskID int64, taskType int) error {
	var (
		req = FinishTaskRequest{
			TaskID:   taskID,
			WorkerID: workerID,
			TaskType: taskType,
		}
		resp = FinishTaskResponse{}
	)
	ok := call("Coordinator.FinishTask", &req, &resp)
	if !ok {
		return errors.New("finish task error")
	}
	return nil
}

// func getMapOutput(mapWorkerID, reduceSplit int64) [][]byte {
// 	// implemented with local filesystem
// 	matches, err := filepath.Glob(fmt.Sprintf("./%d/map-out-%d-*", mapWorkerID, reduceSplit))
// 	if err != nil {
//
// 	}
// }

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

func MakeWorker(workerID int64) {
	sockname := workerSock(workerID)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = rand.Intn(100)

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("request.X:%v, reply.Y %v\n", args.X, reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
