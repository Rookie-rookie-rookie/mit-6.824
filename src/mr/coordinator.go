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

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	DistPhase         Phase
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	InfoMap           map[int]*TaskInfo
	files             []string
}

type TaskInfo struct {
	state     State
	StartTime time.Time
	TaskAdr   *Task
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		InfoMap:           make(map[int]*TaskInfo, len(files)+nReduce),
	}
	c.initMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) initMapTasks(files []string) {

	for _, v := range files {
		id := c.getTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			File:       []string{v},
		}
		Info := TaskInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.InfoMap[Info.TaskAdr.TaskId] = &Info
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) DistributeTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
			} else {
				reply.TaskType = WaittingTask
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) getTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) MarkDone(checkTask *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch checkTask.TaskType {
	case MapTask:
		{
			info, ok := c.InfoMap[checkTask.TaskId]
			if ok && info.state == Working {
				info.state = Done
				fmt.Println("the map task [", checkTask.TaskId, "] is done")
			} else {
				fmt.Println("the map task [", checkTask.TaskId, "] is alread done!")
			}
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}
