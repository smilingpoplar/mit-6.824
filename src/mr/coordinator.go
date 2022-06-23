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
	// Your definitions here.
	sync.Mutex
	phase     TaskPhase
	fileNames []string
	nReduce   int

	fileIdsTodo   []int
	fileIdsDone   map[int]bool
	reduceIdsTodo []int
	reduceIdsDone map[int]bool
}

const taskTimeout = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SendTask(args *struct{}, reply *TaskReply) error {
	c.Lock()
	defer c.Unlock()

	reply.TaskPhase = PhaseWait
	switch c.phase {
	case PhaseMap: // step 1.2
		if len(c.fileIdsDone) < len(c.fileNames) {
			if len(c.fileIdsTodo) > 0 {
				reply.TaskPhase = PhaseMap
				id := c.fileIdsTodo[0]
				c.fileIdsTodo = c.fileIdsTodo[1:]
				reply.Task = MapTask{
					FileId:   id,
					FileName: c.fileNames[id],
					NReduce:  c.nReduce,
				}
				time.AfterFunc(taskTimeout, func() {
					c.checkMapTask(id)
				})
			}
		}
	case PhaseReduce: // step 2.2
		if len(c.reduceIdsDone) < c.nReduce {
			if len(c.reduceIdsTodo) > 0 {
				reply.TaskPhase = PhaseReduce
				id := c.reduceIdsTodo[0]
				c.reduceIdsTodo = c.reduceIdsTodo[1:]
				reply.Task = ReduceTask{
					ReduceId: id,
					NFile:    len(c.fileNames),
				}
				time.AfterFunc(taskTimeout, func() {
					c.checkReduceTask(id)
				})
			}
		}
	case PhaseDone:
		reply.TaskPhase = PhaseDone
	}
	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *struct{}) error {
	c.Lock()
	defer c.Unlock()

	switch args.TaskPhase {
	case PhaseMap: // step 1.5
		fmt.Printf("done: map-%v\n", args.Id)
		c.fileIdsDone[args.Id] = true
		if len(c.fileIdsDone) == len(c.fileNames) {
			c.reduceIdsTodo = make([]int, c.nReduce)
			for i := range c.reduceIdsTodo {
				c.reduceIdsTodo[i] = i
			}
			c.phase = PhaseReduce
		}
	case PhaseReduce: // step 2.5
		fmt.Printf("done: reduce-%v\n", args.Id)
		c.reduceIdsDone[args.Id] = true
		if len(c.reduceIdsDone) == c.nReduce {
			c.phase = PhaseDone
		}
	}
	return nil
}

func (c *Coordinator) checkMapTask(id int) {
	c.Lock()
	defer c.Unlock()

	if !c.fileIdsDone[id] {
		c.fileIdsTodo = append(c.fileIdsTodo, id)
	}
}

func (c *Coordinator) checkReduceTask(id int) {
	c.Lock()
	defer c.Unlock()

	if !c.reduceIdsDone[id] {
		c.reduceIdsTodo = append(c.reduceIdsTodo, id)
	}
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
	c.Lock()
	defer c.Unlock()

	return c.phase == PhaseDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = PhaseMap
	c.fileNames = files
	c.nReduce = nReduce
	c.fileIdsTodo = make([]int, len(files))
	for i := range c.fileIdsTodo {
		c.fileIdsTodo[i] = i
	}
	c.fileIdsDone = make(map[int]bool)
	c.reduceIdsDone = make(map[int]bool)

	c.server()
	return &c
}
