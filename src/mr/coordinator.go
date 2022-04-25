package mr

import (
	"context"
	"log"
	"sync"
	"time"
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

// 整个Job所处的状态
type JobStatus int
const (
	mapStatus JobStatus = iota
	reduceStatus
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

	// 锁
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Apply(request *ApplyRequest, response *ApplyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	isBlocked := true
	if (c.jobStatus == mapStatus) {
		for i := 0; i < c.nMap; i++ {
			if (c.mapTasks[i].taskStatus == taskIdle) {
				response.Id = i
				response.Jobstatus = c.jobStatus
				response.File = c.files[i]
				response.NReduce = c.nReduce
				response.NMap = c.nMap
				response.IsBlocked =  false
				c.mapTasks[i].taskStatus = taskWorking
				isBlocked = false
				// 后台协程监视worker是否超时
				// go c.monitorForTimeOut(&c.mapTasks[i])
				break
			}
		}

		// 没有需要做的任务, 因此让worker阻塞
		if isBlocked {
			response.IsBlocked = isBlocked
		}

	} else if (c.jobStatus == reduceStatus) {
		for i := 0; i < c.nReduce; i++ {
			if (c.reduceTasks[i].taskStatus == taskIdle) {
				response.Id = i
				response.Jobstatus = c.jobStatus
				response.NReduce = c.nReduce
				response.NMap = c.nMap
				response.IsBlocked =  false
				c.reduceTasks[i].taskStatus = taskWorking

				isBlocked = false;
				break
			}
		}

		// go c.monitorForTimeOut(&c.reduceTasks[i])
		if isBlocked {
			response.IsBlocked = isBlocked
		}

	} else {
		// waiting或者exit
		response.Jobstatus = c.jobStatus
	}
	return nil
}

func (c *Coordinator) Commit(request *CommitRequest, response *CommitResponse) error {
	log.Printf("receive commit")
	c.mu.Lock()
	defer c.mu.Unlock()
	switch request.Jobstatus {
	case mapStatus:
		log.Printf("receive commit for map task %v", request.Id)
		if c.mapTasks[request.Id].taskStatus == taskIdle {
			log.Printf("worker Commit task %v after timeout", request.Id)
			return nil
		}
		c.mapTasks[request.Id].taskStatus =  taskCommit
		log.Printf("map task %v commit successfully", request.Id)
		// 检查是否需要转阶段
		changeCase := true
		for _, task := range c.mapTasks {
			if task.taskStatus != taskCommit {
				changeCase = false
				break
			}
		}
		if changeCase {
			c.jobStatus = reduceStatus
			log.Printf("change status to reduce")
		}
	case reduceStatus:
		log.Printf("receive commit for reduce task %v", request.Id)
		if c.reduceTasks[request.Id].taskStatus != taskWorking {
			// 两种可能: worker超时, 之前超时的worker抢先完成了相关的task
			log.Printf("abandoned worker Commit task %v", request.Id)
			return nil
		}
		c.reduceTasks[request.Id].taskStatus = taskCommit
		log.Printf("reduce task %v commit successfully", request.Id)
		changeCase := true
		for _, task := range c.reduceTasks {
			if task.taskStatus != taskCommit {
				changeCase = false
				break
			}
		}
		if changeCase {
			c.jobStatus = ExitStatus
			log.Printf("change status to exit")
		}
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
	c.mu.Lock()
	ret := (c.jobStatus == ExitStatus)
	c.mu.Unlock()
	time.Sleep(10 * time.Second)
	return ret
}

// helper function that create map and reduce works
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

// 监控worker是否超时
func (c * Coordinator) monitorForTimeOut(task *Task) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second * 20) // 不需要cancel函数
	select {
	case <- ctx.Done():
		c.mu.Lock()
		if task.taskStatus != taskCommit {
			// 没有完成任务
			task.taskStatus = taskIdle // 设置任务为空闲
			log.Printf("[Error]: worker with task %v timeout", task.id)
		}
		c.mu.Unlock()
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
