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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type SortedByKey []KeyValue

// for sorting by key.
func (a SortedByKey) Len() int           { return len(a) }
func (a SortedByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	working := true
	for working {
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				fmt.Println("get a map task [", task.TaskId, "]")
				DoMapTask(mapf, &task)
				fmt.Println("finish the map task [", task.TaskId, "]")
				callDone(&task)
			}
		case ReduceTask:
			{
				fmt.Println("get a reduce task [", task.TaskId, "]")
				DoReduceTask(reducef, &task)
				fmt.Println("finish the reduce task [", task.TaskId, "]")
				callDone(&task)
			}
		case WaittingTask:
			{
				time.Sleep(time.Second * 5)
			}
		case ExitTask:
			{
				fmt.Println("all the task done,time for exit")
				working = false
			}
		}
	}

	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func getTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.DistributeTask", &args, &reply)
	if ok {
		//fmt.Println("worker get ", reply.TaskType, "task :Id[", reply.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var intermediate []KeyValue
	filename := task.File[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	intermediate = mapf(filename, string(content))

	rn := task.ReducerNum
	hashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		hashedKV[ihash(kv.Key)%rn] = append(hashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}

func SortReduceFile(files []string) []KeyValue {
	var kvs []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	sort.Sort(SortedByKey(kvs))
	return kvs
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	reduceNum := task.TaskId
	intermediate := SortReduceFile(task.File)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	name := fmt.Sprintf("mr-out-%d", reduceNum)
	os.Rename(tempFile.Name(), name)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

// callDone Call RPC to mark the task as completed
func callDone(f *Task) Task {

	args := f
	reply := Task{}
	ok := call("Coordinator.MarkDone", &args, &reply)

	if ok {
		//fmt.Println("worker finish :taskId[", args.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
