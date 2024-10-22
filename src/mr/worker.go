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
	
	for {
		// Call Coordinator to get a task
		requestArgs := RequestTaskReply{}
		requestReply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &requestArgs, &requestReply)
		if ok {
			// fmt.Printf("event=Receive-Task info=%+v\n", requestReply)
		} else {
			// fmt.Printf("event=call-RequestTask-failed!\n")
			return
		}

		if requestReply.TaskType == None {
			// fmt.Printf("event=Recieve-None-Task\n")
			continue
		}

		taskId := requestReply.TaskId
		taskType := requestReply.TaskType
		var mapOutPutFiles []string
		if taskType == Map {
			// Do a Map Task
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
		
			// // Sort intermediate key-value pairs
			// sort.Sort(ByKey(intermediate))
		
			// Prepare buckets for reduce tasks
			bucket := make([][]KeyValue, requestReply.NumReduceTasks)
			for _, kv := range intermediate {
				bucketIdx := ihash(kv.Key) % requestReply.NumReduceTasks
				bucket[bucketIdx] = append(bucket[bucketIdx], kv)
			}
		
			// Write intermediate results to files
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
				mapOutPutFiles = append(mapOutPutFiles, finalFileName)
			}
		} else if taskType == Reduce {
			var intermediate []KeyValue

			for _, filename := range requestReply.TaskInputFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d", taskId)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
		}

		// Notify Coordinator that the task is done
		// fmt.Printf("event=done-task info=%+v\n", taskId)
		taskDoneArgs := TaskDoneArgs{
			TaskType: taskType,
			TaskId:   taskId,
			OutputFiles: nil,

		}
		if taskType == Map {
			taskDoneArgs.OutputFiles = mapOutPutFiles
		} else if taskType == Reduce {
			// taskDoneArgs.OutputFiles = reduceOutputFiles
		}
		taskDoneReply  := TaskDoneReply {
			Success: false,
		}
		ok = call("Coordinator.NotifyTaskDone", &taskDoneArgs, &taskDoneReply)

		if !ok || !taskDoneReply.Success {
			fmt.Printf("event=call-NotifyTaskDone-failed task_type=%s task_id=%+v\n", taskType.String(), taskId)
		}
		// fmt.Printf("event=wait-for-2s-before-new-task\n")
		// time.Sleep(2*time.Second)
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
