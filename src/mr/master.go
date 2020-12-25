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

type Master struct {
	// Your definitions here.
	sync.Mutex
	taskPhase  TaskPhase
	files      []string
	nReduce    int
	mapIds     []int        // 待做的mapTaskId
	mapDone    map[int]bool // 已做完的mapTask
	reduceIds  []int        // 待做的reduceTaskId
	reduceDone map[int]bool // 已做完的reduceTask
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *struct{}, reply *GetTaskReply) error {
	m.Lock()
	defer m.Unlock()

	switch m.taskPhase {
	case TASK_MAP:
		if len(m.mapDone) < len(m.files) {
			if len(m.mapIds) > 0 {
				reply.TaskPhase = TASK_MAP
				id := m.mapIds[0]
				reply.Payload = MapTask{
					TaskId:  id,
					File:    m.files[id],
					NReduce: m.nReduce,
				}
				m.mapIds = m.mapIds[1:]

				go m.waitForTask(TASK_MAP, id)
			} else {
				reply.TaskPhase = TASK_WAIT
			}
		}
	case TASK_REDUCE:
		if len(m.reduceDone) < m.nReduce {
			if len(m.reduceIds) > 0 {
				reply.TaskPhase = TASK_REDUCE
				id := m.reduceIds[0]
				reply.Payload = ReduceTask{
					TaskId: id,
					NMap:   len(m.files),
				}
				m.reduceIds = m.reduceIds[1:]

				go m.waitForTask(TASK_REDUCE, id)
			} else {
				reply.TaskPhase = TASK_WAIT
			}
		}
	case TASK_DONE:
		reply.TaskPhase = TASK_DONE
	}
	return nil
}

func (m *Master) DoneTask(args *DoneTaskArgs, reply *struct{}) error {
	m.Lock()
	defer m.Unlock()

	switch args.TaskPhase {
	case TASK_MAP:
		fmt.Printf("DoneTask: map-%v\n", args.TaskId)
		m.mapDone[args.TaskId] = true
		if len(m.mapDone) >= len(m.files) {
			m.reduceIds = make([]int, m.nReduce)
			for i := range m.reduceIds {
				m.reduceIds[i] = i
			}
			m.taskPhase = TASK_REDUCE
		}
	case TASK_REDUCE:
		fmt.Printf("DoneTask: reduce-%v\n", args.TaskId)
		m.reduceDone[args.TaskId] = true
		if len(m.reduceDone) >= m.nReduce {
			m.taskPhase = TASK_DONE
		}
	}
	return nil
}

func (m *Master) waitForTask(taskPhase TaskPhase, taskId int) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C

	m.Lock()
	defer m.Unlock()

	switch taskPhase {
	case TASK_MAP:
		if !m.mapDone[taskId] {
			m.mapIds = append(m.mapIds, taskId)
		}
	case TASK_REDUCE:
		if !m.reduceDone[taskId] {
			m.reduceIds = append(m.reduceIds, taskId)
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.Lock()
	defer m.Unlock()

	return m.taskPhase == TASK_DONE
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.taskPhase = TASK_MAP
	m.files = files
	m.nReduce = nReduce
	m.mapIds = make([]int, len(files))
	for i := range m.mapIds {
		m.mapIds[i] = i
	}
	m.mapDone = make(map[int]bool)
	m.reduceDone = make(map[int]bool)

	m.server()
	return &m
}
