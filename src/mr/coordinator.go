package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mu sync.Mutex
)


type Coordinator struct {
	// Your definitions here.
	ReduceNum int
	files []string
	TaskID int
	FramePhase Phase
	MapTaskChannel chan* Task
	ReduceTaskChannel chan* Task
	taskInfos map[int]* TaskInfo
}

type TaskInfo{
	state State
	startTime time.time
	TaskAdr *Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files : files
		ReduceNum : nReduce
		FramePhase : MapPhase
		MapTaskChannel : make(chan *Task,len(files))
		ReduceTaskChannel : make(chan *Task,nReduce)
		taskInfos : make(map[int]*TaskInfo,len(files) + nReduce)
	}

	// Your code here.


	c.server()
	return &c
}

//
//get the task id by the taskid self-increase
//
func (c *Coordinator) getTaskID() int {
	id := c.TaskID
	c.TaskID ++ 
	return id
}

//
//Respone to a worker,select a task and
//distribute it to the worker
//
func (c *Coordinator)DistributeTask(args *TaskArgs, reply *TaskReply) error {
	mu.lock()
	defer mu.Unlock()

}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	mu.lock()
	defer mu.Unlock()

	// Your code here.
	if c.Phase == AllDonePhase {
		fmt.Print("All the task has been finish,the coordinator will exit!")
		ret := true
	}

	return ret
}
