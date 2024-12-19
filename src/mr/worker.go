package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % nreduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	flag := true
	for flag {
		flag = RequestTask(mapf, reducef)
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	args := RequesetArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		if reply.NoTaskFlag {
			time.Sleep(time.Second)
		} else if reply.TaskType == MapTask {
			ProcessMapTask(&reply, mapf)
		} else if reply.TaskType == ReduceTask {
			ProcessReduceTask(&reply, reducef)
		} else if reply.TaskType == ExitTask {
			return false
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return true
}

func ProcessMapTask(task *TaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	kva := mapf(task.FileName, string(content))

	intermediate := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduce_task_num := ihash(kv.Key) % task.NReduce
		intermediate[reduce_task_num] = append(intermediate[reduce_task_num], kv)
	}

	for reduce_task_num, kvs := range intermediate {
		tempFile, _ := ioutil.TempFile("", "mr-temp-*")
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
		tempFile.Close()
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, reduce_task_num)
		os.Rename(tempFile.Name(), finalName)
	}
	TaskIsFinished(task.TaskId, task.TaskType)
}

func ProcessReduceTask(task *TaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for map_TaskId := 0; map_TaskId < task.NMap; map_TaskId++ {
		FileName := fmt.Sprintf("mr-%d-%d", map_TaskId, task.TaskId)
		file, err := os.Open(FileName)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	TaskIsFinished(task.TaskId, task.TaskType)
}

func TaskIsFinished(TaskId int, TaskType int) {
	req := TaskFinishRequest{TaskId: TaskId, TaskType: TaskType}
	rep := TaskFinishReply{}
	ok := call("Coordinator.TaskFinish", &req, &rep)

	if !ok {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
