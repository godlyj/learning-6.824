package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map  = 0
	Reduce = 1
	Idle = 2
	Inprocessed = 3
	Completed = 4
	Exit = 5
	Wait = 6
)

var mutex sync.Mutex

type Task struct{
	Input string
	TaskType int
	State int
	NReduce int
	Taskid int
	Output string
	Intermediates []string
	Starttime time.Time
}




type Stack struct {
	maxSize int
	end int
	lock sync.Mutex
	array   []Task
}

//队列初始化
func (this *Stack) initStack(n int) {
	this.maxSize = n
	this.array = make([]Task, n)
	this.end = -1
	return
}

//添加数据到队列
func (this *Stack) AddStack(val Task) (err error) {
	//先判断队列是否已满
	this.lock.Lock()
	if this.end == (this.maxSize-1) {
		this.lock.Unlock()
		return errors.New("Stack full")
	}
	this.end++
	this.array[this.end] = val
	this.lock.Unlock()
	return
}

//从队列中取出数据，不包含front
func (this *Stack) GetStack()(val Task,err error){
	//判断队列是否为空
	this.lock.Lock()
	if this.end==-1{
		var n Task
		this.lock.Unlock()
		return n,errors.New("Stack empty")
	}

	val = this.array[this.end]
	this.end--
	this.lock.Unlock()
	return val,err
}


func (this *Stack) print()(){

	this.lock.Lock()
	if this.end==-1{
		this.lock.Unlock()
		fmt.Println("no member in Stack")
		return
	}

	for i:=0;i<=this.end;i++{
		fmt.Print(this.array[i].Taskid," ")
	}
	fmt.Println("")
	this.lock.Unlock()

}

func (this *Stack) isnull()(val bool){
	//判断队列是否为空
	this.lock.Lock()
	if this.end==-1{
		this.lock.Unlock()
		return true
	}
	this.lock.Unlock()
	return false
}



type Master struct {
	// Your definitions here.
	Workstack Stack
	TaskMeta	map[int]*Task
	Masterstate   int             // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	s := "????????"
	reply.Input  = s
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	//result := strings.Join(files, ",")
	//
	//// 输出结果\
	//fmt.Println("filename:")
	//fmt.Println(result) // "apple,banana,cherry"
	// Your code here.

	m.InitMaster(files,nReduce)
	m.StartMap()

	m.server()
	go m.detectcrash()
	return &m
}


func max(a int,b int)(val int){
	if a>b{
		return a
	}else{
		return b
	}
}



func (m *Master) detectcrash() {
	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		if m.Masterstate == Exit {
			mutex.Unlock()
			return
		}
		for _, task := range m.TaskMeta {
			if task.State == Inprocessed && time.Now().Sub(task.Starttime) > 10*time.Second {
				m.Workstack.AddStack(*task)
				task.State = Idle
			}
		}
		mutex.Unlock()
	}
}


func (m *Master) InitMaster(files []string, nReduce int) {
	m.InputFiles = files
	m.NReduce = nReduce
	m.Workstack.initStack(max(nReduce,len(files)))
	m.TaskMeta = make(map[int]*Task)
	m.Masterstate = Map
	m.Intermediates = make([][]string, nReduce)
}

//func Copy(source *Task,target *Task){
//	target.TaskType = source.TaskType
//	target.taskid = source.taskid
//	target.Input = source.Input
//	target.NReduce = source.NReduce
//	target.State = source.State
//}


func (m *Master) StartMap() {
	for index,filename :=range m.InputFiles{
		//fmt.Println(index)
		tasktem := Task{
			Input: filename,
			TaskType: Map,
			State: Idle,
			NReduce: m.NReduce,
			Taskid: index,
		}
		m.Workstack.AddStack(tasktem)
		m.TaskMeta[index] = &tasktem
	}
	//fmt.Println(len(m.TaskMeta))
	//for _,value := range m.TaskMeta {
	//	fmt.Println(value.taskid)
	//	fmt.Println(value.Input)
	//	fmt.Println(value.State)
	//}
	//for index,_ :=range m.InputFiles{
	//	fmt.Printf("task%d filename:%s ,State: %d\n",index,m.TaskMeta[index].Input,m.TaskMeta[index].State)
	//}
}


func (m *Master) StartReduce() {
	for index,filenames :=range m.Intermediates{
		//fmt.Println(index)
		tasktem := Task{
			TaskType: Reduce,
			State: Idle,
			NReduce: m.NReduce,
			Taskid: index,
			Intermediates: filenames,
		}
		m.Workstack.AddStack(tasktem)
		m.TaskMeta[index] = &tasktem
	}
	//fmt.Println(len(m.TaskMeta))
	//for _,value := range m.TaskMeta {
	//	fmt.Println(value.taskid)
	//	fmt.Println(value.Input)
	//	fmt.Println(value.State)
	//}
	//for index,_ :=range m.Intermediates{
	//	fmt.Printf("task%d filename:%s ,State: %d\n",index,m.TaskMeta[index].Input,m.TaskMeta[index].State)
	//}
}




func (m *Master) Distribute(args *ExampleArgs, reply *Task) error{
	mutex.Lock()
	defer mutex.Unlock()

	if !m.Workstack.isnull(){
		*reply,_ = m.Workstack.GetStack()
		//fmt.Println("distribute ID:",reply.Taskid)
		//fmt.Println(reply.Taskid)
		//fmt.Println(reply.NReduce)
		//fmt.Println(reply.State)
		//fmt.Println(reply.TaskType)
		//fmt.Println(reply.Input)
		//fmt.Println(len(reply.Input))

		m.TaskMeta[reply.Taskid].State = Inprocessed
		m.TaskMeta[reply.Taskid].Starttime = time.Now()
	} else if m.Masterstate==Exit{
		*reply = Task{
			TaskType: Exit,
		}
	}else{
		*reply = Task{
			TaskType: Wait,
		}
	}

	//m.Workstack.print()
	return nil
}

//func (m *Master) Distribute(args *ExampleArgs, reply *ExampleReply) error {
//	//reply.Y = args.X + 1
//	return nil
//}

func (m *Master)Getask(task *Task, reply *ExampleReply) error{
	mutex.Lock()
	defer mutex.Unlock()

	m.TaskMeta[task.Taskid].State = Completed
	//for index,_ :=range m.InputFiles{
	//	fmt.Printf("task%d filename:%s ,State: %d\n",index,m.TaskMeta[index].Input,m.TaskMeta[index].State)
	//}

	switch{
	case task.TaskType==Map:
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		//fmt.Println("Get Intermediates information")
		//fmt.Println(m.Intermediates)
		if m.Taskdown(){
			m.Masterstate=Reduce
			m.StartReduce()
		}

	case task.TaskType==Reduce:
		if m.Taskdown(){
			m.Masterstate=Exit
		}
	}
	return nil
}

func (m *Master)Taskdown()bool {
	for _, task := range m.TaskMeta {
		if task.State != Completed {
			return false
		}
	}
	return true

}