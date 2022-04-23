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
		switch response.jobStatus{
		case mapStatus:
			mapTask(mapf, response)
		case reduceStatus:
			reduceTask(reducef, response)
		case WaitingStatus:
			time.Sleep(time.Second * 5)
		case ExitStatus:
			os.Exit(0)
		}
	}

}

// 执行map过程
func mapTask(mapf func(string, string) []KeyValue, response applyResponse) {
	// 得到文件内容
	filename := response.file
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
	buffer := make([][]KeyValue, response.nReduce)
	for i := 0; i < len(kva); i++  {
		hash := ihash(kva[i].Key)
		buffer[hash] = append(buffer[hash], kva[i])
	}

	for i := 0; i < response.nReduce; i++ {
		writeToTempFile(response.id, i, &buffer[i])
	}
}

// 把buffer对应的内容写入tempfile
func writeToTempFile(mapNum int, reduceNum int, buffer *[]KeyValue) {
	dir, _ := os.Getwd()

	file, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("worker cannot create temp file")
	}

	enc := json.NewEncoder(file)
	for _, kv := range *buffer {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("worker cannot write to tempfile")
		}
	}
	file.Close()

	oname := ""
	fmt.Sprintf(oname, "mr-tmp-%v-%v", mapNum, reduceNum)
	os.Rename(file.Name(), oname)
}

// reduce过程
func reduceTask(reducef func(string, []string) string, response applyResponse) {
	oname := ""
	fmt.Sprintf(oname, "mr-out-%v", response.id)
	ofile, _ := os.Create(oname) // 结果字符串

	intermediate := readFromLocalFile(response.nMap, response.id)
	sort.Sort(ByKey(intermediate))
	// 在每个不同的key上进行reduce操作
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
	}

}

func readFromLocalFile(nMap int, reduceNum int) []KeyValue {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		oname := ""
		fmt.Sprintf(oname, "mr-tmp-%v-%v", i, reduceNum)
		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("reduce worker cannot open file %v", oname)
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

func CallApply() applyResponse{
	args := applyRequest{}
	response := applyResponse{}
	call("Coordinator.apply", &args, &response)
	return response
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
