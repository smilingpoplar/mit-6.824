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
	// Your worker implementation here.
	for {
		reply := GetTaskReply{}
		call("Master.GetTask", &struct{}{}, &reply)

		switch reply.TaskPhase {
		case TASK_MAP:
			doMap(reply.Payload.(*MapTask), mapf)
		case TASK_REDUCE:
			doReduce(reply.Payload.(*ReduceTask), reducef)
		case TASK_DONE:
			fmt.Printf("tasks all done\n")
			return
		default:
			fmt.Printf("wait other workers\n")
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) {
	// 将kv对输出成nReduce个中间文件
	content := readFile(task.File)
	kva := mapf(task.File, content)
	kvaForNReduce := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		iReduce := ihash(kv.Key) % task.NReduce
		kvaForNReduce[iReduce] = append(kvaForNReduce[iReduce], kv)
	}
	for i, kva := range kvaForNReduce {
		saveToJSON(kva, intermFilename(task.TaskId, i))
	}
	args := DoneTaskArgs{TaskPhase: TASK_MAP, TaskId: task.TaskId}
	call("Master.DoneTask", &args, &struct{}{})
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(task *ReduceTask, reducef func(string, []string) string) {
	// 读取中间文件
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		kva := loadFromJSON(intermFilename(i, task.TaskId))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	filename := fmt.Sprintf("mr-out-%d", task.TaskId)
	atomicWrite(filename, func(file *os.File) {
		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
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
			fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
	})

	// 通知master任务完成
	args := DoneTaskArgs{TaskPhase: TASK_REDUCE, TaskId: task.TaskId}
	call("Master.DoneTask", &args, &struct{}{})
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func intermFilename(mapTaskId int, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

func atomicWrite(filename string, writef func(file *os.File)) {
	// atomic写文件：先写到临时文件，再重命名
	tmp, err := ioutil.TempFile("", filename)
	if err != nil {
		log.Fatalf("cannot open tempfile for %v, err %v", filename, err)
	}
	defer tmp.Close()

	writef(tmp)

	if err := os.Rename(tmp.Name(), filename); err != nil {
		log.Fatal("rename for %v, err: %v", filename, err)
	}
}

func saveToJSON(kva []KeyValue, filename string) {
	atomicWrite(filename, func(file *os.File) {
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v, err: %v", kv, err)
			}
		}
	})
}

func loadFromJSON(filename string) (kva []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
