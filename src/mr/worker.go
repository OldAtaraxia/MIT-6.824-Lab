package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		response := CallApply()
		if response.IsBlocked {
			time.Sleep(time.Second)
			continue
		}
		switch response.Jobstatus {
		case mapStatus:
			log.Printf("worker receive map task %v", response.Id)
			mapTask(mapf, response)
		case reduceStatus:
			log.Printf("worker receive reduce task %v", response.Id)
			reduceTask(reducef, response)
		case ExitStatus:
			os.Exit(0)
		}
	}

}

// 执行map过程
func mapTask(mapf func(string, string) []KeyValue, response ApplyResponse) {
	// 得到文件内容
	filename := response.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("map worker cannot open file %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("map worker cannot read file %v", filename)
	}
	file.Close()

	// 执行map函数
	kva := mapf(filename, string(content))


	// 写入临时文件
	buffer := make([][]KeyValue, response.NReduce)
	for i := 0; i < len(kva); i++  {
		hash := ihash(kva[i].Key) % response.NReduce
		buffer[hash] = append(buffer[hash], kva[i])
	}

	for i := 0; i < response.NReduce; i++ {
		writeToFile(response.Id, i, &buffer[i])
	}

	log.Printf("map worker %v start to commit to coordinator", response.Id)
	CallCommit(CommitRequest{
		Id:        response.Id,
		Jobstatus: response.Jobstatus,
	})
}

// 把buffer对应的内容写入tempfile
func writeToFile(mapNum int, reduceNum int, buffer *[]KeyValue) {
	// dir, _ := os.Getwd()
	// file, err := ioutil.TempFile(dir, "mr-tmp-*")
	//if err != nil {
	//	log.Fatalf("worker cannot create temp file")
	//}

	oname := fmt.Sprintf("mr-tmp-%v-%v", mapNum, reduceNum)
	file, err := os.Create(oname)
	if err != nil {
		log.Fatalf("worker cannot create file %v", oname)
	}

	enc := json.NewEncoder(file)
	for _, kv := range *buffer {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("worker cannot write to file")
		}
	}
	file.Close()

	// oname := fmt.Sprintf("mr-tmp-%v-%v", mapNum, reduceNum)
	//err = os.Rename(file.Name(), oname)
	//if err != nil {
	//	log.Fatalf("err when renameing file: %v", err)
	//}
}

// reduce过程
func reduceTask(reducef func(string, []string) string, response ApplyResponse) {
	//dir, _ := os.Getwd()
	//file, err := ioutil.TempFile(dir, "mr-out-*")
	//if err != nil {
	//	log.Printf("work with reduce task %v cannot create tempfile", response.Id)
	//}

	oname := fmt.Sprintf("mr-out-%v", response.Id)
	file, err := os.Create(oname)
	if err != nil {
		log.Fatalf("reduce worker cannot create file %v", oname)
	}

	intermediate := readFromLocalFile(response.NMap, response.Id)
	sort.Sort(ByKey(intermediate))
	// 在每个不同的key上进行reduce操作
	i := 0
	for i < len(intermediate) {
		j := i + 1 // 第一个与i的key不相等的位置
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	file.Close()
	//oname := fmt.Sprintf("mr-out-%v", response.Id)
	//os.Rename(tempFile.Name(), oname)

	log.Printf("reduce worker %v start to commit to coordinator")
	CallCommit(CommitRequest{
		Id:        response.Id,
		Jobstatus: response.Jobstatus,
	})
}

func readFromLocalFile(nMap int, reduceNum int) []KeyValue {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		oname := fmt.Sprintf("mr-tmp-%v-%v", i, reduceNum)
		file, err := os.Open(oname)
		if err != nil {
			log.Printf("reduce worker cannot open file %v", oname)
		}
		// read from json file
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
	return kva
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

func CallApply() ApplyResponse {
	args := ApplyRequest{}
	response := ApplyResponse{}
	call("Coordinator.Apply", &args, &response)
	return response
}

func CallCommit(request CommitRequest) {
	response := CommitResponse{}
	log.Printf(" commit request %v", request)
	call("Coordinator.Commit", &request, &response)
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
