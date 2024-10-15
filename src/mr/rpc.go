package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task int
const (
	Map Task = iota
	Reduce
)

type Status int
const (
	Unassigned Status = iota
	Assigned
	Done
)

type MRTask struct {
	TaskType   	Task
	Status 		Status
	Index       int
	AssignedAt   time.Time
	InputFiles  []string
	OutputFiles []string
}

// Add your RPC definitions here.
type RequestTaskReply struct {
	TaskId 	 	   int
	TaskType 	   Task
	NumReduceTasks int	
	TaskInputFiles []string
}

type TaskDoneArgs struct {
	TaskType string
	TaskId   int
}

type TaskDoneReply struct {
	Success bool
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
