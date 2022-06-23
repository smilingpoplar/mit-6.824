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

// Add your RPC definitions here.
type TaskPhase int

const (
	PhaseWait TaskPhase = iota
	PhaseMap
	PhaseReduce
	PhaseDone
)

func (tp TaskPhase) String() string {
	return []string{"WAIT", "MAP", "REDUCE", "DONE"}[tp]
}

type TaskReply struct {
	TaskPhase TaskPhase
	Task      interface{}
}

// Task类型必须在encoding/gob编解码前进行注册
func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

type MapTask struct {
	FileId   int
	FileName string
	NReduce  int
}

type ReduceTask struct {
	ReduceId int
	NFile    int
}

type DoneTaskArgs struct {
	TaskPhase TaskPhase
	Id        int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
