package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type SortingByKey []KeyValue

// for sorting by key.
func (a SortingByKey) Len() int           { return len(a) }
func (a SortingByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortingByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
			case Maping:{
				fmt.Print("begin to mapping")
				DoMapTask()
				fmt.Print("finish mapping")
			}
			case Waiting:{
				fmt.Print("waiting for a worker")
				time.Sleep(time.Second * 3)
			}
			case Reducing:{
				fmt.Print("begin to reducing")
				DoReduceTask()
				fmt.Print("finish reducing")
			}
			case Exiting:{
				fmt.Print("finish all work,ready to exit")

			}
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	get := call("",&args,&reply)
	if get {
		fmt.Print("get a",reply.TaskType,"task,ID:[",reply.TaskID,"]")
	} else {
		fmt.Print("fail to get a task")
	}
	return reply
}
//
//deal with the map work
//almost same as mrsequential.main
//
func DoMapTask(mapf func(string, string) []KeyValue,task *Task) {
	var intermediate []KeyValue
	filename := task.FileSlice[0]
	file,err = os.Open(filename)
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
	hashKV := make([][]KeyValue,rn)
	for _, kv := range intermediate {
		hashKV[ihash(kv.Key)%rn] = append(hashKV[ihash(kv.Key)%rn], kv)
	}
	for i:= 0,i < rn; i++ {
		oname = "mr-temp-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		ofile,_ = os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}


//
//deal with the reduce work
//
func DoReduceTask(reducef func(string, []string) string,task *Task) {
	reduceNum = task.ReduceNum
	intermediate = sortFile(tas.FileSlice)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	newName := fmt.Sprintf("mr-out-%d", reduceNum)
	os.Rename(tempFile,)
}

func sortFile(files []string) []KeyValue {
	var ret : []KeyValue
	for _,filepath = range files {
		file,_ = os.Open(filepath) 
		dec := json.NewDecoder(file)
  		for {
    		var kv KeyValue
    		if err := dec.Decode(&kv); err != nil {
      			break
    		}
    		kva = append(kva, kv)
  		}
		file.Close()
	}
	sort.Sort(SortingByKey(ret))
	return ret
}
