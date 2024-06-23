package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TaskReq struct {
	WorkerId int
}

type Task struct {
	Type       int    //系统状态类型
	ID         int    //任务ID
	ReducerNum int    //reducer数量
	FileName   string //文件名
}

type CheckReq struct {
	Task     int //任务ID
	Type     int //任务类型
	WorkerId int //worker id
}

type CheckResp struct {
	Success bool //无关紧要，可以为空结构体
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
//是函数调用的入口
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := rand.Int()
	fmt.Println("new worker ", workerId)
	// Your worker implementation here.
	for true {
		task := CallPullTask(workerId)
		fmt.Printf("task %+v\n", task)
		switch task.Type {
		case Success:
			return
		case Waiting:
			time.Sleep(2 * time.Second)
		case MapStatus:
			{
				mapSolve(workerId, mapf, task)
			}
		case ReduceStatus:
			{
				reduceSolve(workerId, reducef, task)
			}
		default:
			return
		}
		time.Sleep(time.Second * 5)
	}
	// uncomment to send the Example RPC to the coordinator.

}

func mapSolve(workerId int, mapf func(string, string) []KeyValue, task Task) {
	filename := task.FileName
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Printf("open file error, worker 72, filename %v, %v", filename, err)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("read file to content error, worker 77, %v", err)
		return
	}
	kvSlice := mapf(filename, string(content))
	reduceNum := task.ReducerNum
	HashKv := make([][]KeyValue, reduceNum)
	for _, kv := range kvSlice {
		HashKv[ihash(kv.Key)%reduceNum] = append(HashKv[ihash(kv.Key)%reduceNum], kv)
	}
	for i := 0; i < reduceNum; i++ {
		tempFile, err := ioutil.TempFile("", "temp"+strconv.Itoa(rand.Int()))
		if err != nil {
			log.Printf("map creat file error, worker 89 %v", err)
			return
		}
		for _, kv := range HashKv[i] {
			fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
		}
		oldName := tempFile.Name()
		err = os.Rename(oldName, "../"+strconv.Itoa(task.ID)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Printf("rename file error, worker 99 %v", err)
			tempFile.Close()
			return
		}
		tempFile.Close()
	}
	req := CheckReq{
		Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerId,
	}
	CallCheck(req)
}

func reduceSolve(workerId int, reducef func(string, []string) string, task Task) {
	id := strconv.Itoa(task.ID)
	files, err := ioutil.ReadDir("../")
	if err != nil {
		log.Printf("read dir error, worker 116 %v", err)
		return
	}
	kv := make([]KeyValue, 0)
	mp := make(map[string][]string)
	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), id) {
			continue
		}
		file, err := os.Open("../" + fileInfo.Name())
		if err != nil {
			log.Printf("file open error, worker 127 %v\n", err)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Printf("file read all error, worker 132  %v\n", err)
			return
		}
		strContent := string(content)
		strSlice := strings.Split(strContent, "\n")
		for _, row := range strSlice {
			kvSlice := strings.Split(row, " ")
			if len(kvSlice) == 2 {
				mp[kvSlice[0]] = append(mp[kvSlice[0]], kvSlice[1])
			}
		}
	}
	for key, value := range mp {
		kv = append(kv, KeyValue{
			Key:   key,
			Value: reducef(key, value),
		})
	}
	newFile, err := ioutil.TempFile("", "temp"+strconv.Itoa(rand.Int()))
	if err != nil {
		log.Printf("creat file error, worker 157 %v\n", err)
		return
	}
	sort.Sort(SortedKey(kv))
	for _, v := range kv {
		fmt.Fprintf(newFile, "%v %v\n", v.Key, v.Value)
	}
	oldName := newFile.Name()
	defer newFile.Close()
	err = os.Rename(oldName, "mr-out-"+strconv.Itoa(task.ID))
	if err != nil {
		log.Printf("rename error worker 163 , %v\n", err)
		return
	}
	CallCheck(CheckReq{
		Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerId,
	})
}

func SortedKey(kv []KeyValue) sort.Interface {
	return &sortableKeyValue{kv}
}

type sortableKeyValue struct {
	kva []KeyValue
}

func (skv *sortableKeyValue) Len() int {
	return len(skv.kva)
}

func (skv *sortableKeyValue) Less(i, j int) bool {
	return skv.kva[i].Key < skv.kva[j].Key
}

func (skv *sortableKeyValue) Swap(i, j int) {
	skv.kva[i], skv.kva[j] = skv.kva[j], skv.kva[i]
}

// GetTask()从Coordinator获取任务
// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	args := Task{}
	reply := Task{}
	call("Coordinator.AllocateTask", &args, &reply)

	//if !ok {
	//	fmt.Print("GetTask() failed...\n")
	//} else {
	//	fmt.Printf("we get task: %d tasktype: %d\n", reply.TaskID, reply.TaskType)
	//}
	return reply
}

func CallPullTask(Id int) Task {
	req := TaskReq{
		WorkerId: Id,
	}
	reply := Task{}
	ok := call("Coordinator.PullTask", &req, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallCheck(req CheckReq) {
	reply := CheckResp{}
	ok := call("Coordinator.SuccessCheck", &req, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
