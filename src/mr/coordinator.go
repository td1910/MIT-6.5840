package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"

type Coordinator struct {
	NumReduceTasks  int
	MapTasksLeft    int
	ReduceTasksLeft int

	InputFiles 	      []string
	MapTasks          []MRTask
	// MapOutputFiles    []string
	ReduceTasks       []MRTask
	// ReduceOutputFiles []string

	Mutex sync.Mutex
	
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestTask(args *RequestTaskReply, reply *RequestTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for i, task := range c.MapTasks {
		if task.Status == Unassigned {
			// update task info
			c.MapTasks[i].Status = Assigned
        	c.MapTasks[i].UpdatedAt = time.Now()

			reply.TaskId = i
			reply.TaskType = task.TaskType
			reply.NumReduceTasks = c.NumReduceTasks
			reply.TaskInputFiles = task.InputFiles
			fmt.Printf("here?????????? %v\n", task.Status)
			fmt.Printf("here?????????? %v\n", c.MapTasks[0])

			fmt.Printf("event=Map-Task-Assigned task_id=%v\n", i)
			return nil  // Return after assigning one task
		}
	}

	// no Map task left, so try to assign a Reduce task
	// for i, task := range c.ReduceTasks {
	// 	if task.Status == Unassigned {
	// 		// update task info
	// 		task.Status = Assigned
	// 		task.UpdatedAt = time.Now()

	// 		reply.TaskId = i
	// 		reply.TaskType = task.TaskType
	// 		reply.NumReduceTasks = c.NumReduceTasks
	// 		reply.TaskInputFiles = task.InputFiles
			
	// 		return nil  // Return after assigning one task
	// 	}
	// }
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
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filesLength := len(files)
	c := Coordinator{
		InputFiles: files,
		NumReduceTasks: nReduce,
		MapTasks: make([]MRTask, filesLength),
		ReduceTasks: make([]MRTask, nReduce),
		MapTasksLeft: filesLength,
		ReduceTasksLeft: nReduce,
	}

	// load map tasks
	for i := range c.MapTasks {
		c.MapTasks[i] = MRTask{
			TaskType: Map,
			Status: Unassigned,
			Index: i,
			UpdatedAt: time.Now(),
			InputFiles: []string{files[i]},
			OutputFiles: nil,
		}
	}

	// load reduce tasks
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = MRTask{
			TaskType: Reduce,
			Status: Unassigned,
			Index: i,
			UpdatedAt: time.Now(),
			InputFiles: generateReduceInputFiles(i, filesLength),
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}

	c.server()
	return &c
}

func generateReduceInputFiles(i int, numFiles int) []string {
	var inputFiles []string
	for j := 0; j < numFiles; j++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", i, j))
	}
	return inputFiles
}
