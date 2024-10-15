package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "encoding/json"
// import "time"
import "os"
import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Call Coordinator to get a task
	requestArgs := RequestTaskReply{}
	requestReply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &requestArgs, &requestReply)
	if ok {
		fmt.Printf("event=Receive-Task info=%+v\n", requestReply)
	} else {
		fmt.Printf("event=call-RequestTask-failed!\n")
		return
	}

	intermediate := []KeyValue{}
	for _, filename := range requestReply.TaskInputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// Sort intermediate key-value pairs
	sort.Sort(ByKey(intermediate))

	// Prepare buckets for reduce tasks
	bucket := make([][]KeyValue, requestReply.NumReduceTasks)
	for _, kv := range intermediate {
		bucketIdx := ihash(kv.Key) % requestReply.NumReduceTasks
		bucket[bucketIdx] = append(bucket[bucketIdx], kv)
	}

	// Write intermediate results to files
	taskId := requestReply.TaskId
	for reduceTaskId := 0; reduceTaskId < requestReply.NumReduceTasks; reduceTaskId++ {
		// Create a temporary file
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*", taskId, reduceTaskId))
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}

		// Encode data to the temporary file
		enc := json.NewEncoder(tempFile)
		for _, kv := range bucket[reduceTaskId] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode kv pair: %v", err)
			}
		}

		// Close the temporary file
		if err := tempFile.Close(); err != nil {
			log.Fatalf("cannot close temp file: %v", err)
		}

		// Atomically rename the temporary file to the final file name
		finalFileName := fmt.Sprintf("mr-%d-%d", taskId, reduceTaskId)
		if err := os.Rename(tempFile.Name(), finalFileName); err != nil {
			log.Fatalf("cannot rename temp file: %v", err)
		}
	}

	// Notify Coordinator that the task is done
	fmt.Printf("event=done-task info=%+v\n", taskId)
	taskDoneArgs := TaskDoneArgs{
		TaskType: "Map", // This should be dynamic based on the task
		TaskId:   taskId,
	}
	var taskDoneReply TaskDoneReply
	ok = call("Coordinator.NotifyTaskDone", &taskDoneArgs, &taskDoneReply)
	if !ok {
		fmt.Printf("event=call-NotifyTaskDone-failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallRPC() {


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

func generateMapOutputFiles(taskId int, numReduceTasks int) []string {
	var fileNames []string
	for i := 0; i < numReduceTasks; i++ {
		fileNames = append(fileNames, fmt.Sprintf("mr-%d-%d", taskId, i))
	}
	return fileNames
}