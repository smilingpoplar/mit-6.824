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
		// 向coordinator发心跳要任务
		rpcname := "Coordinator.SendTask"
		reply := TaskReply{}
		if ok := call(rpcname, &struct{}{}, &reply); !ok { // step 1.1, step 2.1
			log.Fatalf("call %v failed\n", rpcname)
		}

		switch reply.TaskPhase {
		case PhaseMap: // step 1.3
			doMap(reply.Task.(*MapTask), mapf)
		case PhaseReduce: // step 2.3
			doReduce(reply.Task.(*ReduceTask), reducef)
		case PhaseDone:
			fmt.Printf("done: worker\n")
			return
		default:
			time.Sleep(time.Second) // 心跳间隔
		}
	}
}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) {
	// 将kv对输出成nReduce个中间文件
	content := readFile(task.FileName)
	kva := mapf(task.FileName, content)
	kvaForReduce := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % task.NReduce
		kvaForReduce[reduceId] = append(kvaForReduce[reduceId], kv)
	}
	for reduceId, kva := range kvaForReduce {
		saveToJSON(kva, intermFileName(task.FileId, reduceId))
	}

	args := DoneTaskArgs{TaskPhase: PhaseMap, Id: task.FileId}
	call("Coordinator.DoneTask", &args, &struct{}{}) // step 1.4
}

func readFile(fileName string) string {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file %v", fileName)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read file %v", fileName)
	}
	return string(buf)
}

func intermFileName(fileId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", fileId, reduceId)
}

func saveToJSON(kva []KeyValue, fileName string) {
	atomicWriteFile(fileName, func(file *os.File) {
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v, err: %v", kv, err)
			}
		}
	})
}

func atomicWriteFile(fileName string, writef func(file *os.File)) {
	// 先写到临时文件，再重命名
	tmp, err := ioutil.TempFile("", fileName)
	if err != nil {
		log.Fatalf("cannot open tempfile for %v, err %v", fileName, err)
	}
	defer tmp.Close()

	writef(tmp)

	if err := os.Rename(tmp.Name(), fileName); err != nil {
		log.Fatalf("rename for %v, err: %v", fileName, err)
	}
}

func loadFromJSON(fileName string) (kva []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
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

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(task *ReduceTask, reducef func(string, []string) string) {
	// 合并中间文件
	intermediate := []KeyValue{}
	for i := 0; i < task.NFile; i++ {
		kva := loadFromJSON(intermFileName(i, task.ReduceId))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	fileName := fmt.Sprintf("mr-out-%d", task.ReduceId)
	writeReduceFile(fileName, intermediate, reducef)

	args := DoneTaskArgs{TaskPhase: PhaseReduce, Id: task.ReduceId}
	call("Coordinator.DoneTask", &args, &struct{}{}) // 2.4
}

func writeReduceFile(fileName string, kva []KeyValue, reducef func(string, []string) string) {
	atomicWriteFile(fileName, func(file *os.File) {
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

			i = j
		}
	})
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
