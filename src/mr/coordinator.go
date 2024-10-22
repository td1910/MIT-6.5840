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
		if task.Status == Unassigned || (task.Status == Assigned && time.Since(c.MapTasks[i].AssignedAt) > 5*time.Second) {
			// update task info
			c.MapTasks[i].Status = Assigned
        	c.MapTasks[i].AssignedAt = time.Now()

			reply.TaskId = i
			reply.TaskType = task.TaskType
			reply.NumReduceTasks = c.NumReduceTasks
			reply.TaskInputFiles = task.InputFiles

			fmt.Printf("event=Map-Task-Assigned task_id=%v\n", i)
			return nil  // Return after assigning one task
		}
	}
	if c.MapTasksLeft == 0 {
		// no Map task left, so try to assign a Reduce task
		for i, task := range c.ReduceTasks {
			if task.Status == Unassigned || (task.Status == Assigned && time.Since(c.ReduceTasks[i].AssignedAt) > 5*time.Second) {
				// update task info
				c.ReduceTasks[i].Status = Assigned
				c.ReduceTasks[i].AssignedAt = time.Now()

				reply.TaskId = i
				reply.TaskType = task.TaskType
				reply.NumReduceTasks = c.NumReduceTasks
				reply.TaskInputFiles = task.InputFiles

				fmt.Printf("event=Reduce-Task-Assigned task_id=%v\n", i)
				return nil  // Return after assigning one task
			}
		}
	}


	// No task left
	reply.TaskType = None
	return nil
}

func (c *Coordinator) NotifyTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	switch args.TaskType {
	case Map:
		if c.MapTasks[args.TaskId].Status != Done {
			c.MapTasks[args.TaskId].Status = Done

			for reduceTaskId := 0; reduceTaskId < c.NumReduceTasks; reduceTaskId++ {
				outputFile := args.OutputFiles[reduceTaskId]
				c.ReduceTasks[reduceTaskId].InputFiles = append(c.ReduceTasks[reduceTaskId].InputFiles, outputFile)
			}
			c.MapTasksLeft--
			fmt.Printf("event=done-map-task task_id=%+v\n", args.TaskId)
		}
	case Reduce:
		if c.ReduceTasks[args.TaskId].Status != Done {
			c.ReduceTasks[args.TaskId].Status = Done
			c.ReduceTasks[args.TaskId].OutputFiles = args.OutputFiles
			c.ReduceTasksLeft--
			fmt.Printf("event=done-reduce-task task_id=%+v\n", args.TaskId)

		}
	default:
		return fmt.Errorf("event=Notify-invalid-task-type: %s", args.TaskType.String())
	}
	reply.Success = true
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.MapTasksLeft == 0 && c.ReduceTasksLeft == 0 {
		fmt.Println("Done-all-tasks")
		return true
	}
	return false
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
			AssignedAt: time.Now(),
			InputFiles: []string{files[i]},
			OutputFiles: nil, // will be fetched when done i-th map task
		}
	}

	// load reduce tasks
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = MRTask{
			TaskType: Reduce,
			Status: Unassigned,
			Index: i,
			AssignedAt: time.Now(),
			InputFiles: nil, // will be fetched when done i-th map task
			OutputFiles: nil, // will be fetched when done i-th reduce tasks
		}
	}

	c.server()
	fmt.Printf("event=coordinator-ready\n")
	return &c
}