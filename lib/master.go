package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Status string

var (
	unstarted  Status = "unstarted"
	inProgress Status = "inProgress"
	completed  Status = "completed"
)

type TaskMeta struct {
	Number    int
	StartTime time.Time
	Status    Status
}

type Master struct {
	MapTasks        map[string]*TaskMeta
	ReduceTasks     map[string]*TaskMeta
	CondVar         *sync.Cond
	MapRemaining    int
	ReduceRemaining int
	NumReduce       int
}

func (m *Master) GetMapTask() (string, int) {
	for t := range m.MapTasks {
		if m.MapTasks[t].Status == unstarted {
			m.MapTasks[t].StartTime = time.Now().UTC()
			m.MapTasks[t].Status = inProgress
			return t, m.MapTasks[t].Number
		}
	}
	return "", 0
}

func (m *Master) GetReduceTask() (string, int) {
	for t := range m.ReduceTasks {
		if m.ReduceTasks[t].Status == unstarted {
			m.ReduceTasks[t].StartTime = time.Now().UTC()
			m.ReduceTasks[t].Status = inProgress
			return t, m.ReduceTasks[t].Number
		}
	}
	return "", 0
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.CondVar.L.Lock()

	if m.MapRemaining != 0 {
		mTask, mNumber := m.GetMapTask()
		for mTask == "" {
			if m.MapRemaining == 0 {
				break
			}
			m.CondVar.Wait()
			mTask, mNumber = m.GetMapTask()
		}
		if mTask != "" {
			reply.Name = mTask
			reply.Number = mNumber
			reply.Type = mapType
			reply.NReduce = m.NumReduce
			m.CondVar.L.Unlock()
			return nil
		}
	}

	if m.ReduceRemaining != 0 {
		rTask, rNumber := m.GetReduceTask()
		for rTask == "" {
			if m.MapRemaining == 0 {
				m.CondVar.L.Unlock()
				return errors.New("task completed, no more tasks")

			}
			m.CondVar.Wait()
			rTask, rNumber = m.GetReduceTask()
		}
		if rTask != "" {
			reply.Name = rTask
			reply.Number = rNumber
			reply.Type = reduceType
			reply.NReduce = m.NumReduce
			m.CondVar.L.Unlock()
			return nil
		}
	}

	m.CondVar.L.Unlock()
	return errors.New("No tasks available")
}

func (m *Master) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	m.CondVar.L.Lock()
	if args.Type == mapType {
		m.MapTasks[args.Name].Status = completed
		m.MapRemaining--
	} else {
		m.ReduceTasks[args.Name].Status = completed
		m.ReduceRemaining--
	}
	m.CondVar.L.Unlock()
	return nil
}

func (m *Master) rescheduler() {
	for {
		m.CondVar.L.Lock()

		if m.MapRemaining != 0 {
			for task := range m.MapTasks {
				currTime := time.Now().UTC()
				startTime := m.MapTasks[task].StartTime
				status := m.MapTasks[task].Status
				if status == inProgress {
					diff := currTime.Sub(startTime).Seconds()
					if diff > 10 {
						log.Printf("Rescheduling task with name '%s', type '%s'", task, mapType)
						m.MapTasks[task].Status = unstarted
						m.CondVar.Broadcast()
					}
				}
			}
		} else if m.ReduceRemaining != 0 {
			for task := range m.ReduceTasks {
				currTime := time.Now().UTC()
				startTime := m.ReduceTasks[task].StartTime
				status := m.ReduceTasks[task].Status
				if status == inProgress {
					diff := currTime.Sub(startTime).Seconds()
					if diff > 10 {
						log.Printf("Rescheduling task with name '%s', type '%s'", task, reduceType)
						m.ReduceTasks[task].Status = unstarted
						m.CondVar.Broadcast()
					}
				}
			}
		} else {
			m.CondVar.Broadcast()
			m.CondVar.L.Unlock()
			break
		}

		m.CondVar.L.Unlock()
	}
}
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	socketName := masterSocket()
	os.Remove(socketName)
	listener, err := net.Listen("unix", socketName)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	go http.Serve(listener, nil)
}

func (m *Master) Done() bool {
	retVal := false
	m.CondVar.L.Lock()
	if m.ReduceRemaining == 0 {
		retVal = true
	}
	defer m.CondVar.L.Unlock()
	return retVal
}

func MakeMaster(files []string, numReduce int) *Master {
	mapTasks := map[string]*TaskMeta{}
	for i, file := range files {
		mapTasks[file] = &TaskMeta{Number: i, Status: unstarted}
	}
	reduceTasks := map[string]*TaskMeta{}
	for i := 0; i < numReduce; i++ {
		reduceTasks[strconv.Itoa(i)] = &TaskMeta{Number: i, Status: unstarted}
	}
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	m := Master{
		MapTasks:        mapTasks,
		ReduceTasks:     reduceTasks,
		MapRemaining:    len(files),
		ReduceRemaining: numReduce,
		NumReduce:       numReduce,
		CondVar:         cond,
	}

	go m.rescheduler()
	m.server()
	return &m
}
