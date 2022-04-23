package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Task和Job的状态
type TaskStatus int
const (
	taskIdle TaskStatus = iota
	taskWorking
	taskCommit
)

type JobStatus int
const (
	mapStatus JobStatus = iota
	reduceStatus
	WaitingStatus
	ExitStatus
)

type Task struct {
	id int
	taskStatus TaskStatus
	file string
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	nMap int
	jobStatus JobStatus

	// 分别维护两个Task列表
	mapTasks []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) apply(args *applyRequest, response *applyResponse) error {
	if (c.jobStatus == mapStatus) {
		for i := 0; i < c.nMap; i++ {
			if (c.mapTasks[i].taskStatus == taskIdle) {
				response.id = i
				response.jobStatus = c.jobStatus
				response.file = c.files[i]
				response.nReduce = c.nReduce
				c.mapTasks[i].taskStatus = taskWorking
			}
		}
	} else if (c.jobStatus == reduceStatus) {
		for i := 0; i < c.nReduce; i++ {
			if (c.reduceTasks[i].taskStatus == taskIdle) {
				response.id = i
				response.jobStatus = c.jobStatus
				response.nReduce = c.nReduce
				c.reduceTasks[i].taskStatus = taskWorking
			}
		}
	} else {
		// waiting或者exit
		response.jobStatus = c.jobStatus
	}
	return nil
}

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
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.


	return ret
}

func (c *Coordinator) createWork() {
	for i := 0; i < c.nMap; i++ {
		c.mapTasks = append(c.mapTasks, Task{
			id: i,
			taskStatus: taskIdle,
			file: c.files[i],
		})
	}

	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			id: i,
			taskStatus: taskIdle,
		})
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		nMap: len(files),
		jobStatus: mapStatus,
	}

	c.createWork()


	c.server()
	return &c
}
