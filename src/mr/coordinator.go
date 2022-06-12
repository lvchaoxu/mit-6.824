package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	MapTasks           []Task
	ReduceTasks        []Task
	IdleWorkers        map[string]bool
	MapAllocatedNum    int
	MapCompletedNum    int
	ReduceCompletedNum int
	ReduceAllocatedNum int
	mu                 sync.Mutex
}

func (c *Coordinator) MapAllCompleted() bool {
	return c.MapCompletedNum == len(c.MapTasks)
}
func (c *Coordinator) MapAllAllocated() bool {
	return c.MapAllocatedNum == len(c.MapTasks)
}
func (c *Coordinator) ReduceAllCompleted() bool {
	return c.ReduceCompletedNum == len(c.ReduceTasks)
}
func (c *Coordinator) ReduceAllAllocated() bool {
	return c.ReduceAllocatedNum == len(c.ReduceTasks)
}
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	exists := c.IdleWorkers[args.Url]
	if exists {
		reply.Ok = false
		reply.Err = fmt.Errorf("worker %v has registered!", args.Url)
		return fmt.Errorf("worker %v has registered!", args.Url)
	} else {
		reply.Ok = true
		c.IdleWorkers[args.Url] = true
		return nil
	}
}

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//log.Printf("%v请求任务", args.Url)
	c.mu.Lock()
	defer c.mu.Unlock()
	//Map任务没有分配完
	if c.MapAllAllocated() == false {
		for taskNumber, task := range c.MapTasks {
			if task.Allocated == false {
				c.IdleWorkers[args.Url] = false
				c.MapTasks[taskNumber].Allocated = true
				c.MapTasks[taskNumber].AllocatedTime = time.Now()
				c.MapTasks[taskNumber].Worker = args.Url
				c.MapAllocatedNum++
				reply.Task = c.MapTasks[taskNumber]
				reply.Err = nil
				break
			}
		}
	} else if c.MapAllCompleted() && !c.MapAllCompleted() { //Map分配结束但是所有Map没有结束，让worker等待所有map task结束
		reply.Task = Task{TaskNumber: -1, Type: "map"}
		reply.Err = nil
	} else if c.ReduceAllAllocated() == false { //Map任务全部结束，Reduce任务还没有分配完
		for taskNumber, task := range c.ReduceTasks {
			if task.Allocated == false {
				c.IdleWorkers[args.Url] = false
				c.ReduceTasks[taskNumber].Allocated = true
				c.ReduceTasks[taskNumber].AllocatedTime = time.Now()
				c.ReduceTasks[taskNumber].Worker = args.Url
				c.ReduceAllocatedNum++
				reply.Task = c.ReduceTasks[taskNumber]
				reply.Err = nil
				break
			}
		}
	} else if c.ReduceAllAllocated() && !c.ReduceAllCompleted() { //Reduce任务全部分配结束，但是没有全部完成
		reply.Task = Task{TaskNumber: -1, Type: "reduce"}
		reply.Err = nil
	} else { //reduce任务全部完成，没有map或者reduce task来分配
		reply.Err = fmt.Errorf("没有map或者reduce tasks来分配")
	}
	if reply.Err == nil {
		return nil
	} else {
		return fmt.Errorf("没有map或者reduce tasks来分配")
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	//log.Printf("%v report %v type %v task done", args.Url, args.Type, args.TaskNumber)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case "map":
		taskNumber := args.TaskNumber
		//taskNumber不在数组范围内或者已计数(非重复)
		if taskNumber < 0 || taskNumber >= len(c.MapTasks) || c.MapTasks[taskNumber].Done {
			break
		}
		c.MapCompletedNum++
		c.MapTasks[taskNumber].Done = true
	case "reduce":
		taskNumber := args.TaskNumber
		if taskNumber < 0 || taskNumber >= len(c.ReduceTasks) || c.ReduceTasks[taskNumber].Done {
			break
		}
		c.ReduceCompletedNum++
		c.ReduceTasks[taskNumber].Done = true
	}
	reply.Err = nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.MapAllCompleted() && c.ReduceAllCompleted()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]Task, len(files))
	c.ReduceTasks = make([]Task, nReduce)
	c.IdleWorkers = make(map[string]bool)
	c.MapCompletedNum = 0
	c.MapAllocatedNum = 0
	c.ReduceCompletedNum = 0
	c.ReduceAllocatedNum = 0
	for i := 0; i < len(files); i++ {
		c.MapTasks[i] = Task{
			TaskNumber: i,
			Type:       "map",
			Files:      []string{files[i]},
			Allocated:  false,
			Done:       false,
			MMap:       len(files),
			NReduce:    nReduce,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskNumber: i,
			Type:       "reduce",
			Files:      []string{},
			Allocated:  false,
			Done:       false,
			MMap:       len(files),
			NReduce:    nReduce,
		}
	}
	c.server()
	return &c
}
