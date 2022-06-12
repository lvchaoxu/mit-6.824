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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

func HandleMapTask(mapf func(string, string) []KeyValue, task *Task) {
	//fmt.Printf("开始处理Map任务%v", task.TaskNumber)
	intermediate := make([][]KeyValue, task.NReduce)
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("os open %v failed", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("os read file %v error", file)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			bucketId := ihash(kv.Key) % task.NReduce
			intermediate[bucketId] = append(intermediate[bucketId], kv)
		}
	}
	//写到中间文件中 mr-out-x-y
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-out-%v-%v", task.TaskNumber, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("fatal error")
			}
		}
		ofile.Close()
	}
}

func HandleReduceTask(reducef func(string, []string) string,
	task *Task) {
	intermediate := []KeyValue{}
	for mapTaskNumber := 0; mapTaskNumber < task.MMap; mapTaskNumber++ {
		iname := fmt.Sprintf("mr-out-%v-%v", mapTaskNumber, task.TaskNumber)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
		os.Remove(iname)
	}
	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	ofile, _ := os.Create(oname)

	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//log.Println("worker启动了")
	Register()
	//log.Println("注册成功")
	for {
		task := RequestTask()
		if task == nil {
			break
		} else if task.TaskNumber < 0 {
			//log.Printf("等待%v阶段结束\n", task.Type)
			time.Sleep(time.Second)
			continue
		}
		//log.Printf("申请到%v类型的编号为%v的任务", task.Type, task.TaskNumber)
		if task.Type == "map" {
			HandleMapTask(mapf, task)
		} else {
			HandleReduceTask(reducef, task)
		}
		ReplyTaskDone(task.TaskNumber, task.Type)
	}
}

func Register() {
	args := RegisterWorkerArgs{
		Url: string(os.Getpid()),
	}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		log.Println("register successfully")
	} else {
		log.Fatal("called to the server failed")
	}
}

func RequestTask() *Task {
	args := RequestTaskArgs{
		Url: string(os.Getpid()),
	}
	reply := RequestTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return &reply.Task
	} else {
		log.Fatal("request task from server failed")
		return nil
	}
}

func ReplyTaskDone(TaskNumber int, Type string) {
	args := TaskDoneArgs{
		Url:        string(os.Getpid()),
		TaskNumber: TaskNumber,
		Type:       Type,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		//fmt.Println("reply task done")
	} else {
		log.Fatal("reply task to coordinator failed")
	}
}

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
