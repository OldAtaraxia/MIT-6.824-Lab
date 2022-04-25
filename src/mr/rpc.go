package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ApplyRequest struct {
	// worker请求coordinator分配task
	// ...感觉也不需要什么参数
}

type ApplyResponse struct {
	// coordinator应答worker分配task的请求
	Id        int
	File      string
	Jobstatus JobStatus
	NReduce   int
	NMap      int
	IsBlocked bool // 告诉worker是否需要阻塞
}

type CommitRequest struct {
	// worker告知coordinator任务已经完成
	Id        int
	Jobstatus JobStatus
}

type CommitResponse struct {
	// ...感觉也不需要什么参数
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
