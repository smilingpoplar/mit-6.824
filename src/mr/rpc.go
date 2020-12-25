package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskPhase int

const (
	TASK_MAP TaskPhase = iota
	TASK_REDUCE
	TASK_DONE
	TASK_WAIT
)

func (p TaskPhase) String() string {
	switch p {
	case TASK_MAP:
		return "MAP"
	case TASK_REDUCE:
		return "REDUCE"
	case TASK_DONE:
		return "DONE"
	default:
		return "WAIT"
	}
}

type GetTaskReply struct {
	TaskPhase TaskPhase
	Payload   interface{}
}

// Payload类型必须在encoding/gob编码解码前进行注册
func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

type MapTask struct {
	TaskId  int
	File    string
	NReduce int
}

type ReduceTask struct {
	TaskId int
	NMap   int
}

type DoneTaskArgs struct {
	TaskPhase TaskPhase
	TaskId    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
