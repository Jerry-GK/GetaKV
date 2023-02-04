package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

//common
const Debug = false

const (
	TaskStatusReady   = 0 //just created, waited to be put into tack Channel of the master
	TaskStatusQueue   = 1 //waiting for an idle worker's request to execute it
	TaskStatusRunning = 2 //running in some worker 
	TaskStatusDone    = 3 //done by the worker
	TaskStatusErr     = 4 //err in the worker
)

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type TaskPhase int
type TaskStatus int
type Task struct{
	FileName string //empty for reduce tasks
	NumMap int
	NumReduce int
	Seq int
	Phase TaskPhase
	Active bool

	Status    TaskStatus
	WorkerId  int //-1 if not assigned yet
	StartTime time.Time 
}

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf("[Debug Msg] " + format+"\n", v...)
	}
}

func intermediateName(mapSeq, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapSeq, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
//common end

const (
	MaxTaskRunTime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type Master struct {
	// Your definitions here.
	files []string
	mu sync.Mutex
	done bool
	TotalNumReduce int
	maxWorkerId int
	globalTaskPhase TaskPhase
	taskArray []Task
	taskChan chan Task
}

//maintain taskArray, assign tasks to workers
//called at regular time, called when a worker sends a report
func (m *Master) Schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done{
		return
	}
	var allTaskDone bool = true
	for idx, t := range m.taskArray{
		switch t.Status{
		case TaskStatusReady:
			allTaskDone = false
			m.taskChan <- m.taskArray[idx]
			m.taskArray[idx].Status = TaskStatusQueue
		case TaskStatusQueue:
			allTaskDone = false
		case TaskStatusRunning:
			allTaskDone = false
			//check timeout
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskArray[idx].Status = TaskStatusQueue
				m.taskChan <- m.taskArray[idx]
				DPrintf("[Global-Redo-Timeout] Redo task-%d", idx)
			}
		case TaskStatusErr:
			allTaskDone = false
			m.taskArray[idx].Status = TaskStatusQueue
			m.taskChan <- m.taskArray[idx]
			DPrintf("[Global-Redo-TaskErr] Redo task-%d", idx)
		case TaskStatusDone:
		default:
			DPrintf("[Master-Failure] unknown task status")
		}
	}
	
	if allTaskDone{
		if m.globalTaskPhase == MapPhase{
			//all map tasks done, go to reduce phase
			m.globalTaskPhase = ReducePhase
			DPrintf("[Global-Phase] All map tasks done, go to reduce phase")
			time.Sleep(time.Second)
			m.InitReduceTasks()
		}else{
			//both map and reduce tasks are all done
			DPrintf("[Global-Phase] All reduce tasks done, over")
			time.Sleep(time.Second)
			m.done = true
		}
	}
}

func (m *Master) GenerateOneTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NumReduce:  m.TotalNumReduce,
		NumMap:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.globalTaskPhase,
		Active:    true,

		Status:	TaskStatusReady,
		WorkerId: -1,
		StartTime: time.Now(),
	}
	m.taskArray[taskSeq] = task // right?
	DPrintf("[Master] generate task: taskseq = %d", taskSeq)
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

func (m *Master) InitMapTasks() {
	DPrintf("[Master] init map tasks")
	m.globalTaskPhase = MapPhase
	m.taskArray = make([]Task, len(m.files))
	for i := 0; i < len(m.taskArray); i++{
		m.taskArray[i] = m.GenerateOneTask(i)
	}
}

func (m *Master) InitReduceTasks() {
	//after all map tasks are done, do this
	DPrintf("[Master] init reduce tasks")
	m.globalTaskPhase = ReducePhase
	m.taskArray = make([]Task, m.TotalNumReduce)
	for i := 0; i < len(m.taskArray); i++{
		m.taskArray[i] = m.GenerateOneTask(i) //Seq?
	}
}

func (m *Master) RegOneTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.globalTaskPhase {
		DPrintf("[Master-Failure] task phase exception")
	}

	m.taskArray[task.Seq].Status = TaskStatusRunning
	m.taskArray[task.Seq].WorkerId = args.WorkerId
	m.taskArray[task.Seq].StartTime = time.Now()
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskChan
	reply.TaskRep = &task
	if task.Active{
		m.RegOneTask(args, &task)
	}
	//DPrintf("Master gets one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxWorkerId += 1 //the first worker's id is assigned 0
	reply.WorkerId = m.maxWorkerId
	DPrintf("[Master] register worker-%d", m.maxWorkerId)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	DPrintf("[Master-Report] Get task report: %+v, taskPhase = %+v ", args, m.globalTaskPhase)

	m.taskArray[args.Seq].Status = args.Status
	go m.Schedule()
	return nil //master does NOT need reply to workers
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
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := true
	// Your code here.
	ret = m.done
	return ret
}

func (m *Master) TickSchedule() {
	for !m.Done() {
		go m.Schedule() //schedule tasks
		time.Sleep(ScheduleInterval)
	}
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.files = files
	m.done = false
	m.TotalNumReduce = nReduce
	m.maxWorkerId = -1 //the first worker's id is assigned 0
	m.globalTaskPhase = MapPhase

	var chanLen int = m.TotalNumReduce
	if m.TotalNumReduce < len(files){
		chanLen = len(files)	
	}
	m.taskChan = make(chan Task, chanLen)

	m.InitMapTasks()
	go m.TickSchedule() //schedule tasks at regular time

	m.server()
	DPrintf("[Master] start to serve")
	return &m
}
