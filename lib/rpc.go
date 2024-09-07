package mr

import (
	"os"
	"strconv"
)

type TaskType string

var (
	mapType    TaskType = "map"
	reduceType TaskType = "reduce"
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Type    TaskType
	Number  int
	Name    string
	NReduce int
}

type UpdateTaskStatusArgs struct {
	Name string
	Type TaskType
}

type UpdateTaskStatusReply struct {
}

func masterSocket() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
