package mr

import "encoding/json"
import "io/ioutil"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct{
	id int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w := worker{}
	w.id = -1 //-1 means not registered yet
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	DPrintf("[Worker] worker-%d successfully register", w.id)
	w.run()

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func (w *worker) register(){
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Master.RegWorker", &args, &reply)
	if !ok {
		log.Fatal("register worker fail")
	}
	w.id = reply.WorkerId
}

func (w *worker) run(){
	for{
		t := w.reqTask()
		if !t.Active{
			return //or return?
		}
		w.doTask(t)
	}
}

func (w *worker) reqTask() Task{
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}
	ok := call("Master.GetOneTask", &args, &reply)
	if !ok{
		DPrintf("[Worker-Failure] Worker-%d request task fail, exit", w.id)
		os.Exit(1)
	}
	//DPrintf("Worker-%d get task %+v", w.id, reply.TaskRep)
	return *reply.TaskRep
}

func (w *worker) reportTask(t Task, err error) {
	if err != nil {
		DPrintf("[Worker-TaskErr] err = %v", err)
	}
	args := ReportTaskArgs{}
	args.Status = t.Status
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{} //no use

	ok := call("Master.ReportTask", &args, &reply)
	if !ok {
		DPrintf("[Worker-Failure] report task fail:%+v", args)
	}
}

func (w *worker) doTask(t Task){
	DPrintf("[Worker] worker-%+v start task-%+v", w.id, t.Seq)
	
	switch t.Phase{
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		DPrintf("[Worker-Failure] unknown task phase")
	}
}

func (w *worker) doMapTask(t Task){
	//read input file
	file, err := os.Open(t.FileName)
	if err != nil {
		t.Status = TaskStatusErr
		w.reportTask(t, err)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		t.Status = TaskStatusErr
		w.reportTask(t, err)
		return
	}
	file.Close()

	kva := w.mapf(t.FileName, string(content))
	//intermediate = append(intermediate, kva...)

	//partition the intermidiate kv-array into NumReduce regions, each as the input of one reduce worker
	reduces := make([][]KeyValue, t.NumReduce)
	for _, kv := range kva{
		idx := ihash(kv.Key) % t.NumReduce
		reduces[idx] = append(reduces[idx], kv) //kv-array reduce[idx] is the input for reduce worker[idx]
	}
	//write intermidiate pairs into NumReduce files
	for idx, l := range reduces{
		fileName := intermediateName(t.Seq, idx)
		file, err := os.Create(fileName)
		if err != nil {
			t.Status = TaskStatusErr
			w.reportTask(t, err)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range l{
			//write kv pair
			err = enc.Encode(&kv)
			if err != nil {
				t.Status = TaskStatusErr
				w.reportTask(t, err)
				return
			}
		}
		err = file.Close()
		if err != nil {
			t.Status = TaskStatusErr
			w.reportTask(t, err)
			return
		}
	}
	t.Status = TaskStatusDone
	DPrintf("[Worker] worker-%+v has done task %+v", w.id, t.Seq)
	w.reportTask(t, nil)
}

func (w *worker) doReduceTask(t Task){
	//collect all intermidate files whose index belongs to this reduce worker
	intermediate := []KeyValue{}
	for i := 0; i < t.NumMap; i++{
		fileName := intermediateName(i, t.Seq) //only read files that this reduce worker responsible for
		file, err := os.Open(fileName) //in real distributed system, the file may be remote
		if err != nil {
			t.Status = TaskStatusErr
			w.reportTask(t, err)
			return
		}
		dec := json.NewDecoder(file)

		for{
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break //err = EOF
			}
			intermediate = append(intermediate, kv)
		}
	}	

	//sort the kv pairs by key (similiar to main/mrsequential.go)
	sort.Sort(ByKey(intermediate))

	//aggregate by key, call reduce function, generate the output file (similiar to main/mrsequential.go)
	oname := mergeName(t.Seq)
	ofile, _ := os.Create(oname)
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
		output := w.reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	t.Status = TaskStatusDone
	DPrintf("[Worker] worker-%+v has done task %+v", w.id, t.Seq)
	w.reportTask(t, nil)
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

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//DPrintf("start rpc call: rpcname = " + rpcname)
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) //why stall?
	if err == nil {
		//DPrintf("end rpc call without err")
		return true
	}

	fmt.Println(err)
	//DPrintf("end rpc call with err in c.Call")
	return false
}
