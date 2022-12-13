package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
			state:   Working,
			TaskAdr: &task,
		}
		c.InfoMap[Info.TaskAdr.TaskId] = &Info
		c.MapTaskChannel <- &task
	}
}

func selectReduceFile(reduceNum int) []string {
	var ret []string
	filepath, _ := os.Getwd()
	files, _ := ioutil.ReadDir(filepath)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			ret = append(ret, file.Name())
		}
	}
	return ret
}

func (c *Coordinator) initReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		task := Task{
			TaskType: ReduceTask,
			TaskId:   c.getTaskId(),
			File:     selectReduceFile(i),
		}
		info := TaskInfo{
			state:   Working,
			TaskAdr: &task,
		}
		c.InfoMap[info.TaskAdr.TaskId] = &info
		c.ReduceTaskChannel <- &task
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
				fmt.Printf("distribute map taskid[ %d ]\n", reply.TaskId)
			} else {
				reply.TaskType = WaittingTask
				if c.CheckAllTaskDone() {
					fmt.Println("all map tasks finished,begin to init reduce tasks")
					c.DistPhase = ReducePhase
					c.initReduceTasks()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				fmt.Println("distribute reduce task [", reply.TaskId, "]")
			} else {
				reply.TaskType = WaittingTask
				if c.CheckAllTaskDone() {
					fmt.Println("all reduce tasks finished")
					c.DistPhase = AllDone
				}
			}
			return nil
		}
	case AllDone:
		{
			fmt.Println("all the reduce tasks finished")
			reply.TaskType = ExitTask
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
	fmt.Println("mark")
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
	case ReduceTask:
		{
			info, ok := c.InfoMap[checkTask.TaskId]
			if ok && info.state == Working {
				info.state = Done
				fmt.Println("the reduce task [", checkTask.TaskId, "] is done")
			} else {
				fmt.Println("the reduce task [", checkTask.TaskId, "] is alread done!")
			}
		}
	}
	return nil
}

func (c *Coordinator) CheckAllTaskDone() bool {
	var (
		mapDone      = 0
		mapUnDone    = 0
		ReduceDone   = 0
		ReduceUnDone = 0
	)
	for _, info := range c.InfoMap {
		if info.TaskAdr.TaskType == MapTask {
			if info.state == Done {
				mapDone++
			} else {
				mapUnDone++
			}
		} else if info.TaskAdr.TaskType == ReduceTask {
			if info.state == Done {
				ReduceDone++
			} else {
				ReduceUnDone++
			}
		}
	}
	fmt.Println("mapDone:", mapDone)
	fmt.Println("mapUnDone:", mapUnDone)
	fmt.Println("reduceDone:", ReduceDone)
	fmt.Println("reduceUnDone:", ReduceUnDone)

	if (mapDone > 0 && mapUnDone == 0) && (ReduceDone == 0 && ReduceUnDone == 0) {
		return true
	} else if ReduceDone > 0 && ReduceUnDone == 0 {
		return true
	} else {
		return false
	}
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
