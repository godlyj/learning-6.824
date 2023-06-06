package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
    for{
		task := gettask()
		switch{
		case task.TaskType == Map:
			//fmt.Printf("Map Job , ID=%d\n",task.Taskid)
			doMap(mapf,&task)
		case task.TaskType == Reduce:
			//fmt.Println("Reduce Job")
			doReduce(reducef,&task)
		case task.TaskType ==Exit:
			return
		case task.TaskType==Wait:
			continue
		}
	}

	//fmt.Println(task.Taskid)
	//fmt.Println(task.NReduce)
	//fmt.Println(task.State)
	//fmt.Println(task.TaskType)
	//fmt.Println(task.Input)
	//fmt.Println(len(task.Input))

}


func doMap(mapf func(string, string) []KeyValue, task *Task) {
	//fmt.Println(task.Taskid)
	//fmt.Println(task.NReduce)
	//fmt.Println(task.State)
	//fmt.Println(task.TaskType)
	//fmt.Println(task.Input)
	//fmt.Println(len(task.Input))
	//do map job
	intermediate := []KeyValue{}
	file, err := os.Open(task.Input)
	if err != nil {
		log.Fatalf("map cannot open task%d %v", task.Taskid,task.Input)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("map cannot read %v", task.Input)
	}
	file.Close()
	//fmt.Printf("get file! %s\n",task.Input)
	kva := mapf(task.Input, string(content))
	intermediate = append(intermediate, kva...)
	//for key,_ := range intermediate{
	//	fmt.Println(key)
	//}
	//
	//save

	buf := make([][]KeyValue, task.NReduce)
	for _, im := range intermediate {
		index := ihash(im.Key) % task.NReduce
		buf[index] = append(buf[index], im)
	}

	fileintermediate:=make([]string, 0)

	for i := 0; i < task.NReduce; i++ {
		oname :=  fmt.Sprintf("mr-%d-%d", task.Taskid,i)
		//dir, _ := os.Getwd()
		//fmt.Println(dir+"/"+oname)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range buf[i] {
			//fmt.Println(kv)
			err := enc.Encode(&kv)
			if err!=nil{
				log.Fatal("Failed to write kv pair", err)
			}
		}
		ofile.Close()
		fileintermediate = append(fileintermediate, oname)
	}

	//fmt.Println(fileintermediate)
	task.Intermediates = fileintermediate
	returnask(task)
}

func returnask(task *Task){
	args := task
	reply := ExampleReply{}
	// send the RPC request, wait for the reply.
	call("Master.Getask", args, &reply)
}




func doReduce(reducef func(string, []string) string, task *Task) {

	im := []KeyValue{}
	for _, filepath := range task.Intermediates {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			im = append(im, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(im))
	oname :=  fmt.Sprintf("mr-out-%d", task.Taskid)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(im) {
		j := i + 1
		for j < len(im) && im[j].Key == im[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, im[k].Value)
		}
		output := reducef(im[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", im[i].Key, output)

		i = j
	}
	returnask(task)
}





//
// example function to show how to make an RPC call to the master.
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


	call("Master.Example", &args, &reply)

	fmt.Printf("reply.Y %v\n", reply.Y)
	fmt.Printf("reply.Y %s\n", reply.Input)

}

func gettask()(Task) {

	// declare an argument structure.
	args := ExampleArgs{}
	reply := Task{}
	// send the RPC request, wait for the reply.
	call("Master.Distribute", &args, &reply)
	return reply
}



//
// send an RPC request to the master, fmt.Println(reply.filename)wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
