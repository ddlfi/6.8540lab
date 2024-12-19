package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	TaskId     int
	TaskType   int
	TaskStatus int
	FileName   string
	StartTime  time.Time
}

const (
	MapTask = iota
	ReduceTask
	ExitTask
)

const (
	Pending = iota
	Running
	Finished
)

const (
	MapPhase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	Mutex       sync.Mutex
	Phase       int
	MapDone     int
	ReduceDone  int
	NReduce     int
	NMap        int
	MapTasks    []Task
	ReduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) GetTask(args *RequesetArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.Phase == MapPhase {
		for i := range c.MapTasks {
			if c.MapTasks[i].TaskStatus == Running || c.MapTasks[i].TaskStatus == Finished {
				continue
			} else {
				c.MapTasks[i].TaskStatus = Running
				c.MapTasks[i].StartTime = time.Now()
				reply.TaskId = c.MapTasks[i].TaskId
				reply.FileName = c.MapTasks[i].FileName
				reply.TaskType = MapTask
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				return nil
			}
		}
		reply.NoTaskFlag = true
	} else if c.Phase == ReducePhase {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].TaskStatus == Running || c.ReduceTasks[i].TaskStatus == Finished {
				continue
			} else {
				c.ReduceTasks[i].TaskStatus = Running
				c.ReduceTasks[i].StartTime = time.Now()
				reply.TaskId = i
				reply.TaskType = ReduceTask
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				return nil
			}
		}
		reply.NoTaskFlag = true
	} else if c.Phase == DonePhase{
		reply.TaskType = ExitTask
		return nil
	}
	return nil
}

func (c *Coordinator) TaskFinish(req *TaskFinishRequest, reply *TaskFinishReply)error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if req.TaskType == MapTask {
		c.MapTasks[req.TaskId].TaskStatus = Finished
		c.MapDone++
		if c.MapDone == c.NMap {
			c.Phase = ReducePhase
		}
	} else if req.TaskType == ReduceTask {
		c.ReduceTasks[req.TaskId].TaskStatus = Finished
		c.ReduceDone++
		if c.ReduceDone == c.NReduce {
			c.Phase = DonePhase
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.Phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nreduce int) *Coordinator {
	c := Coordinator{
		Phase:       MapPhase,
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nreduce),
		NReduce:     nreduce,
		NMap:        len(files),
	}

	for i, file := range files {
		c.MapTasks[i] = Task{TaskId: i, TaskStatus: Pending, TaskType: MapTask, FileName: file}
	}

	for i := 0; i < nreduce; i++ {
		c.ReduceTasks[i] = Task{TaskId: i, TaskStatus: Pending, TaskType: ReduceTask}
	}
	// Your code here.

	c.server()
	go c.monitorTimeout()
	return &c
}

func (c *Coordinator) monitorTimeout() {
	for {
		time.Sleep(1 * time.Second)
		c.Mutex.Lock()
		now := time.Now()

		// 检查 Map 任务
		for i := range c.MapTasks {
			task := &c.MapTasks[i]
			// if task.TaskStatus == Running{
			// 	fmt.Printf("%v\n",now.Sub(task.StartTime))
			// }
			if task.TaskStatus == Running && now.Sub(task.StartTime) > 10*time.Second {
				log.Printf("Map Task %d timed out, reassigning...\n", i)
				task.TaskStatus = Pending
			}
		}

		// 检查 Reduce 任务
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			// if task.TaskStatus == Running{
			// 	fmt.Printf("%v\n",now.Sub(task.StartTime))
			// }
			if task.TaskStatus == Running && now.Sub(task.StartTime) > 10*time.Second {
				log.Printf("Reduce Task %d timed out, reassigning...\n", i)
				task.TaskStatus = Pending
			}
		}

		c.Mutex.Unlock()
	}
}
