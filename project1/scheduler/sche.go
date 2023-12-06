package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/fsnotify/fsnotify"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
	//"go.etcd.io/etcd/client/pkg/fileutil"
)

const MaxTaskCount = 2

const Bucket = "taskstate"

const taskTimeout = 300

type task struct {
	Id                 string
	MemNeed            int
	CfgPath            string
	ResultPath         string
	WorkerID           string
	Compress           int //1:压缩,0不压缩
	Ok                 bool
	group              *taskGroup
	deadline           time.Time
	continuedSeconds   int
	iterationNum       int
	exploit            float64
	lastChange         int64
	writtingResultFile bool
}

func (t *task) less(o *task) bool {
	if t.MemNeed < o.MemNeed {
		return false
	} else if t.MemNeed == o.MemNeed {
		return t.Id > o.Id
	} else {
		return true
	}
}

func readline(r *bufio.Reader) (line []byte, err error) {
	for {
		var l []byte
		var isPrefix bool
		l, isPrefix, err = r.ReadLine()
		if err != nil {
			return line, err
		}

		line = append(line, l...)

		if !isPrefix {
			return line, err
		}
	}
}

func (t *task) loadCfgFromFile(core int, threadReserved int) (content string, err error) {
	var f *os.File

	f, err = os.Open(t.CfgPath)
	if err != nil {
		return "", err
	}

	defer f.Close()

	reader := bufio.NewReader(f)

	var line []byte
	var lines []string
	for {
		line, err = readline(reader)
		if err == io.EOF {
			err = nil
			content = strings.Join(lines, "\n")
			break
		}

		lineStr := string(line)
		if lineStr != "" {
			if strings.Contains(lineStr, "set_thread_num") {
				thread_num := 1
				if core > threadReserved {
					thread_num = core - threadReserved
				}
				lines = append(lines, "set_thread_num "+fmt.Sprintf("%d", thread_num))
			} else if strings.Contains(lineStr, "dump_result") {
				lines = append(lines, "dump_result "+t.Id+".json")
			} else {
				lines = append(lines, lineStr)
			}
		}
	}

	return content, err

}

func (t *task) save(db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket([]byte(Bucket))
		jsonBytes, _ := json.Marshal(t)
		err = bucket.Put([]byte(t.Id), jsonBytes)
		return err
	})

	if err != nil {
		logger.Sugar().Error(err)
	}
}

type worker struct {
	workerID    string
	memory      int
	threadcount int
	tasks       map[string]*task
	socket      *netgo.AsynSocket
	inAvailable bool
}

func (w *worker) dispatchJob(task *task, ThreadReserved int) {
	go func() {
		cfgContent, err := task.loadCfgFromFile(w.threadcount, ThreadReserved)
		if err != nil {
			logger.Sugar().Errorf("load task:%s cfgfile:%s error:%v", task.Id, task.CfgPath, err)
			return
		}

		msg := &proto.DispatchJob{
			TaskID:   task.Id,
			Cfg:      cfgContent,
			Compress: task.Compress,
		}
		w.socket.Send(msg)
		logger.Sugar().Debugf("dispatch task:%s to worker:%s", task.Id, w.workerID)
	}()
}

type taskGroup struct {
	filepath string
	tasks    []string
}

type sche struct {
	workers           map[string]*worker
	taskGroups        map[string]*taskGroup
	doing             map[string]*task //求解中的task
	tasks             map[string]*task
	unAllocTasks      []*task   //尚未分配执行的任务，按memNeed升序排列
	availableWorkers  []*worker //根据memory按升序排列
	processQueue      chan func()
	die               chan struct{}
	stopc             chan struct{}
	cfg               *Config
	db                *bolt.DB
	pauseFlag         int32 //client暂停工作标记
	dispatchFlag      int32 //暂停分发任务
	check_result_file bool
	f                 *os.File
	taskFailedRecord  *os.File
	c                 chan string
	loadCounter       int
	averageTaskMem    int64
}

func isFileExist(path string, compress bool) bool {
	if compress {
		path += ".zip"
	}
	_, err := os.Stat(path)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (g *taskGroup) loadTaskFromFile(s *sche) error {
	logger.Sugar().Debugf("load %s", g.filepath)

	var f *os.File
	var err error

	f, err = os.Open(g.filepath)
	if err != nil {
		return err
	}

	defer f.Close()

	reader := bufio.NewReader(f)

	var line []byte
	for {
		line, err = readline(reader)
		if err == io.EOF {
			err = nil
			break
		}
		lineStr := string(line)
		fields := strings.Split(lineStr, "\t")

		var compress int

		if len(fields) > 5 && fields[5] == "compress" {
			compress = 1
		}

		t := &task{
			Id:         strings.ReplaceAll(strings.ReplaceAll(fields[0], "<", "{"), ">", "}"),
			CfgPath:    fields[2],
			ResultPath: fields[4],
			Compress:   compress,
			group:      g,
		}

		s.loadCounter++
		logger.Sugar().Debugf("%d loading task %s", s.loadCounter, t.Id)

		t.MemNeed, err = strconv.Atoi(fields[3])
		//特殊处理----------------
		if t.MemNeed == 2 {
			t.MemNeed = 0
		}

		m := t.MemNeed / 10

		if s.cfg.MemoryRevise[m] != 0 {
			logger.Sugar().Debugf("task%d before:%d after:%d", t.Id, t.MemNeed, t.MemNeed+s.cfg.MemoryRevise[m])
		}

		t.MemNeed += s.cfg.MemoryRevise[m]

		s.averageTaskMem += int64(t.MemNeed)

		if err != nil {
			err = fmt.Errorf("(2)invaild task:%s error:%v", lineStr, err)
			break
		}

		if s.check_result_file && isFileExist(t.ResultPath, t.Compress == 1) {
			logger.Sugar().Debugf("%s is finished", t.Id)
			//检查结果文件是否存在，如果存在直接将任务标记为已经完成
			t.Ok = true
			t.save(s.db)
		} else {
			s.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(Bucket))
				v := b.Get([]byte(t.Id))
				if v != nil {
					var tt task
					err := json.Unmarshal(v, &tt)
					if err != nil {
						return err
					}
					t.Ok = tt.Ok
					t.WorkerID = tt.WorkerID
				}
				return nil
			})
		}

		if !t.Ok {
			if t.WorkerID != "" {
				t.deadline = time.Now().Add(time.Second * taskTimeout)
				s.doing[t.Id] = t
			} else {
				s.unAllocTasks = append(s.unAllocTasks, t)
			}
		}

		s.tasks[t.Id] = t

		g.tasks = append(g.tasks, t.Id)
	}

	return err
}

func (s *sche) StopDispatch() {
	atomic.StoreInt32(&s.dispatchFlag, 0)
}

func (s *sche) StartDispatch() {
	atomic.StoreInt32(&s.dispatchFlag, 1)
	s.processQueue <- func() {
		s.tryDispatchJob()
	}
}

func (s *sche) addTaskFile(file string) {
	group := &taskGroup{
		filepath: file,
	}

	if err := group.loadTaskFromFile(s); err == nil {
		s.taskGroups[group.filepath] = group
		s.tryDispatchJob()
	} else {
		logger.Sugar().Errorf("loadTaskFromFile(%s)  error:%v", file, err)
	}
}

func (s *sche) removeTaskFile(file string) {
	g := s.taskGroups[file]
	if g != nil {
		delete(s.taskGroups, file)
		for _, v := range g.tasks {
			if t := s.doing[v]; t != nil {
				delete(s.doing, v)
				if worker := s.workers[t.WorkerID]; worker != nil {
					for _, vv := range worker.tasks {
						if vv.Id == v {
							vv.WorkerID = ""
						}
					}
					worker.socket.Send(&proto.CancelJob{TaskID: v})
				}
			}

			delete(s.tasks, v)
		}

		//重构unAllocTasks
		var unAllocTasks []*task
		for k, t := range s.tasks {
			if !t.Ok && s.doing[k] == nil {
				unAllocTasks = append(unAllocTasks, t)
			}
		}

		sort.Slice(unAllocTasks, func(i, j int) bool {
			return unAllocTasks[i].less(unAllocTasks[j])
		})

		s.unAllocTasks = unAllocTasks

	}
}

func (s *sche) init() error {

	var err error
	{
		f, err := os.OpenFile("ResultFilelist.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			logger.Sugar().Errorf("OpenFile error:%v", err)
			return err
		}

		s.f = f
	}

	{
		f, err := os.OpenFile("TaskFailedRecord.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			logger.Sugar().Errorf("OpenFile error:%v", err)
			return err
		}

		s.taskFailedRecord = f
	}

	s.c = make(chan string, 1024)

	go func() {
		for {
			select {
			case v := <-s.c:
				s.f.WriteString(v)
				s.f.Sync()
			case <-s.die:
				return
			}
		}
	}()

	err = s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(Bucket)) == nil {
			logger.Sugar().Debugln("check_result_file")
			//s.check_result_file = true
		}
		_, err := tx.CreateBucketIfNotExists([]byte(Bucket))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("could not set up buckets, %v", err)
	}

	err = filepath.Walk(s.cfg.TaskCfg, func(filePath string, f os.FileInfo, _ error) error {
		if f != nil && !f.IsDir() {
			group := &taskGroup{
				filepath: filePath,
			}

			if err := group.loadTaskFromFile(s); err == nil {
				s.taskGroups[group.filepath] = group
			} else {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	sort.Slice(s.unAllocTasks, func(i, j int) bool {
		return s.unAllocTasks[i].less(s.unAllocTasks[j])
	})

	s.averageTaskMem /= int64(len(s.tasks))

	//监控s.cfg.TaskCfg
	watch, _ := fsnotify.NewWatcher()
	watch.Add(s.cfg.TaskCfg)

	go func() {
		for {
			select {
			case ev := <-watch.Events:
				{
					if ev.Op&fsnotify.Create == fsnotify.Create {
						file, err := os.Stat(ev.Name)
						if err == nil && !file.IsDir() {
							s.processQueue <- func() {
								s.addTaskFile(ev.Name)
								logger.Sugar().Debugf("add file:%s total unAllocTasks:%d", ev.Name, len(s.unAllocTasks))
							}
						}
					}

					if ev.Op&fsnotify.Remove == fsnotify.Remove {
						s.processQueue <- func() {
							s.removeTaskFile(ev.Name)
							logger.Sugar().Debugf("remove file:%s total unAllocTasks:%d", ev.Name, len(s.unAllocTasks))
						}
					}
				}
			case err := <-watch.Errors:
				{
					logger.Sugar().Errorf("error : %v", err)
					return
				}
			}
		}
	}()

	logger.Sugar().Debugf("task count:%d", len(s.unAllocTasks))

	return nil
}

func (s *sche) onWorkerHeartBeat(socket *netgo.AsynSocket, heartbeat *proto.WorkerHeartBeat) {
	w, _ := socket.GetUserData().(*worker)
	if w == nil {
		w = s.workers[heartbeat.WorkerID]
		if w != nil {
			socket.Close(errors.New("duplicate worker"))
			return
		} else {

			w = &worker{
				workerID:    heartbeat.WorkerID,
				memory:      int(heartbeat.Memory) - s.cfg.MemoryReserved,
				threadcount: int(heartbeat.ThreadCount) / 2,
				socket:      socket,
				tasks:       map[string]*task{},
			}

			logger.Sugar().Debugf("on new worker:%s mem:%d", w.workerID, w.memory)

			socket.SetUserData(w)
			s.workers[w.workerID] = w

			socket.SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
				s.processQueue <- func() {
					logger.Sugar().Debugf("worker:%s disconnected", w.workerID)
					delete(s.workers, w.workerID)
					if w.inAvailable {
						for i, v := range s.availableWorkers {
							if v == w {
								i = i + 1
								for ; i < len(s.availableWorkers); i++ {
									s.availableWorkers[i-1] = s.availableWorkers[i]
								}
								s.availableWorkers = s.availableWorkers[:len(s.availableWorkers)-1]
							}
						}
					}
				}
			})

			heartbeatTasks := map[string]bool{}

			for _, v := range heartbeat.Tasks {
				heartbeatTasks[v.TaskID] = true
				if task := s.doing[v.TaskID]; task != nil && task.WorkerID == w.workerID {
					task.continuedSeconds = v.ContinuedSeconds
					task.iterationNum = v.IterationNum
					if v.Exploit != task.exploit {
						task.lastChange = time.Now().Unix()
					}
					task.exploit = v.Exploit
					w.tasks[task.Id] = task
					logger.Sugar().Debugf("dispatchJob %s to worker:%s mem before:%d mem after:%d task mem:%d", task.Id, w.workerID, w.memory, w.memory-task.MemNeed, task.MemNeed)
					w.memory -= task.MemNeed
					task.deadline = time.Now().Add(time.Second * taskTimeout)
				} else {
					if task := s.tasks[v.TaskID]; task != nil {
						//worker自己上报的，需要先记录资源占用，防止被当成可用worker
						w.tasks[task.Id] = task
						logger.Sugar().Debugf("heartbeat %s worker:%s mem before:%d mem after:%d task mem:%d", task.Id, w.workerID, w.memory, w.memory-task.MemNeed, task.MemNeed)
						w.memory -= task.MemNeed
						if task.Ok && task.WorkerID == w.workerID {
							w.socket.Send(&proto.AcceptJobResult{
								TaskID: v.TaskID,
							})
						} else {
							w.socket.Send(&proto.CancelJob{TaskID: v.TaskID})
						}

					} else {
						logger.Sugar().Errorf("worker:%s heartbeat invaild task:%s", w.workerID, v.TaskID)
					}
				}
			}

			for _, v := range s.doing {
				if _, ok := heartbeatTasks[v.Id]; !ok {
					//检查有没有分配给workerID但没有上报的任务
					if v.WorkerID == w.workerID {
						v.continuedSeconds = 0
						v.iterationNum = 0
						v.exploit = 0

						logger.Sugar().Debugf("dispatchJob %s to worker:%s mem before:%d mem after:%d task mem:%d", v.Id, w.workerID, w.memory, w.memory-v.MemNeed, v.MemNeed)

						w.tasks[v.Id] = v
						w.memory -= v.MemNeed
						v.deadline = time.Now().Add(time.Second * taskTimeout)
						w.dispatchJob(v, s.cfg.ThreadReserved)
					}
				}
			}

			if len(w.tasks) < MaxTaskCount {
				s.onWorkerAvaliable(w, true)
			}
		}
	} else {
		for _, v := range w.tasks {
			i := 0
			for ; i < len(heartbeat.Tasks); i++ {
				vv := heartbeat.Tasks[i]
				if v.Id == vv.TaskID && v.WorkerID == w.workerID {
					v.continuedSeconds = vv.ContinuedSeconds
					v.iterationNum = vv.IterationNum
					if v.exploit != vv.Exploit {
						v.lastChange = time.Now().Unix()
					}
					v.exploit = vv.Exploit
					v.deadline = time.Now().Add(time.Second * taskTimeout)
					break
				}
			}

			if i == len(heartbeat.Tasks) {
				if v.Ok || v.WorkerID != w.workerID {
					delete(w.tasks, v.Id)
					logger.Sugar().Debugf("worker:%s recycle:%s task,task mem:%d, mem before:%d mem after:%d", w.workerID, v.Id, v.MemNeed, w.memory, w.memory+v.MemNeed)
					w.memory += v.MemNeed
				}
			}
		}

		if len(w.tasks) < MaxTaskCount {
			s.onWorkerAvaliable(w, true)
		}
	}
}

func (s *sche) onJobFailed(socket *netgo.AsynSocket, notify *proto.JobFailed) {
	logger.Sugar().Debugf("onJobFailed %v", notify.TaskID)
	if w, _ := socket.GetUserData().(*worker); w != nil {

		if task := s.tasks[notify.TaskID]; task != nil {
			s.taskFailedRecord.WriteString(fmt.Sprintf("task:%s failed resultPath:%s worker:%s\n", notify.TaskID, task.ResultPath, w.workerID))
			s.taskFailedRecord.Sync()
			for _, v := range w.tasks {
				if !v.Ok && notify.TaskID == v.Id && v.WorkerID == w.workerID {
					//通告重新执行
					w.socket.Send(&proto.ReDispatchJob{
						TaskID: notify.TaskID,
					})
				}
			}
			w.socket.Send(&proto.CancelJob{TaskID: notify.TaskID})
		}
	}
}

func (s *sche) onCommitJobResult(socket *netgo.AsynSocket, commit *proto.CommitJobResult) {
	logger.Sugar().Debugf("onCommitJobResult %v", commit.TaskID)
	if w, _ := socket.GetUserData().(*worker); w != nil {
		for _, v := range w.tasks {
			if !v.writtingResultFile && !v.Ok && commit.TaskID == v.Id && v.WorkerID == w.workerID {
				go func() {

					ok := false

					defer func() {
						s.processQueue <- func() {
							v.writtingResultFile = false
							if ok {
								v.Ok = true
								v.save(s.db)
								delete(s.doing, v.Id)

								w.socket.Send(&proto.AcceptJobResult{
									TaskID: commit.TaskID,
								})
							}
						}
					}()

					//将commit.Result写入本地文件
					ResultPath := v.ResultPath

					if v.Compress == 1 {
						ResultPath += ".zip"
					}

					os.MkdirAll(filepath.Dir(ResultPath), 0600)

					f, err := os.OpenFile(ResultPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
					if err != nil {
						logger.Sugar().Errorf("OpenFile error:%v", err)
						return
					}

					defer f.Close()

					byteCount := 0

					if v.Compress == 1 {
						// Base64 Standard Decoding
						var sDec []byte
						sDec, err = base64.StdEncoding.DecodeString(commit.Result)
						if err != nil {
							logger.Sugar().Errorf("Error decoding string: %s ", err.Error())
							return
						}
						byteCount = len(sDec)
						_, err = f.Write(sDec)
					} else {
						byteCount = len(commit.Result)
						_, err = f.WriteString(commit.Result)
					}

					if err != nil {
						logger.Sugar().Errorf("WriteFile error:%v", err)
						return
					}

					err = f.Sync()

					if err != nil {
						logger.Sugar().Errorf("SyncFile error:%v", err)
						return
					}

					ok = true

					s.c <- fmt.Sprintf("path:%s size:%d create:%s\n", ResultPath, byteCount, time.Now().String())

				}()

				return
			}
		}

		if task := s.tasks[commit.TaskID]; task != nil && task.Ok && task.WorkerID == w.workerID {
			w.socket.Send(&proto.AcceptJobResult{
				TaskID: commit.TaskID,
			})
		} else {
			w.socket.Send(&proto.CancelJob{TaskID: commit.TaskID})
		}
	}
}

func (s *sche) dispatchJob(task *task) {
	if atomic.LoadInt32(&s.dispatchFlag) == 1 && atomic.LoadInt32(&s.pauseFlag) == 0 {
		//寻找一个worker将task分配出去，如果没有合适的worker将task放回到unAllocTasks
		for i, v := range s.availableWorkers {
			if v.memory >= task.MemNeed {
				w := v

				task.WorkerID = w.workerID

				task.save(s.db)

				task.deadline = time.Now().Add(time.Second * taskTimeout)

				w.tasks[task.Id] = task

				logger.Sugar().Debugf("dispatchJob %s to worker:%s mem before:%d mem after:%d task mem:%d", task.Id, w.workerID, w.memory, w.memory-task.MemNeed, task.MemNeed)

				w.memory -= task.MemNeed

				//如果memory不足或已经运行了MaxTaskCount数量的任务，将worker从availableWorkers移除
				if len(w.tasks) >= MaxTaskCount {
					w.inAvailable = false
					i = i + 1
					for ; i < len(s.availableWorkers); i++ {
						s.availableWorkers[i-1] = s.availableWorkers[i]
					}
					s.availableWorkers = s.availableWorkers[:len(s.availableWorkers)-1]
				} else {
					sort.Slice(s.availableWorkers, func(i, j int) bool {
						return s.availableWorkers[i].memory < s.availableWorkers[j].memory
					})
				}

				s.doing[task.Id] = task
				w.dispatchJob(task, s.cfg.ThreadReserved)
				return
			}
		}
	}

	//没有合适的worker,将job添加到jobQueue
	task.WorkerID = ""
	task.continuedSeconds = 0
	task.iterationNum = 0
	task.exploit = 0

	delete(s.doing, task.Id)

	task.save(s.db)

	s.unAllocTasks = append(s.unAllocTasks, task)

	sort.Slice(s.unAllocTasks, func(i, j int) bool {
		return s.unAllocTasks[i].less(s.unAllocTasks[j])
	})

}

func (s *sche) start() {
	ticker := time.NewTicker(time.Second)

	go func() {
		for range ticker.C {
			s.processQueue <- func() {
				//从新分配超时任务
				var timeout []*task
				now := time.Now()
				for n, v := range s.doing {
					if now.After(v.deadline) {
						timeout = append(timeout, v)
						delete(s.doing, n)
					}
				}

				for _, v := range timeout {
					logger.Sugar().Debugf("task:%s timeout on worker:%s", v.Id, v.WorkerID)
					if w, ok := s.workers[v.WorkerID]; ok {
						delete(w.tasks, v.Id)
						logger.Sugar().Debugf("worker:%s recycle task:%s task memory:%d,before mem:%d after mem:%d", w.workerID, v.Id, v.MemNeed, w.memory, w.memory+v.MemNeed)
						w.memory += v.MemNeed
						if len(w.tasks) < MaxTaskCount && !w.inAvailable {
							w.inAvailable = true
							s.availableWorkers = append(s.availableWorkers, w)
							sort.Slice(s.availableWorkers, func(i, j int) bool {
								return s.availableWorkers[i].memory < s.availableWorkers[j].memory
							})
						}
					}
					s.dispatchJob(v)
				}
			}
		}
	}()

	for {
		select {
		case task := <-s.processQueue:
			task()
		case <-s.die:
			close(s.stopc)
			return
		}
	}
}

func (s *sche) stop() {
	close(s.die)
	<-s.stopc
}

func (s *sche) onWorkerAvaliable(w *worker, dosort bool) {
	if len(w.tasks) < MaxTaskCount && atomic.LoadInt32(&s.dispatchFlag) == 1 && atomic.LoadInt32(&s.pauseFlag) == 0 {
		last := len(s.unAllocTasks) - 1
		if last >= 0 {
			if s.unAllocTasks[last].MemNeed < 1 {
				for len(w.tasks) < MaxTaskCount && last >= 0 {
					v := s.unAllocTasks[last]
					if v.MemNeed > w.memory {
						break
					}
					logger.Sugar().Debugf("dispatchJob %s to worker:%s mem before:%d mem after:%d task mem:%d", v.Id, w.workerID, w.memory, w.memory-v.MemNeed, v.MemNeed)
					w.memory -= v.MemNeed
					w.tasks[v.Id] = v
					v.WorkerID = w.workerID
					v.save(s.db)
					v.deadline = time.Now().Add(time.Second * taskTimeout)
					s.doing[v.Id] = v
					w.dispatchJob(v, s.cfg.ThreadReserved)
					s.unAllocTasks = s.unAllocTasks[:len(s.unAllocTasks)-1]
					last = len(s.unAllocTasks) - 1
				}
			} else if w.memory >= s.unAllocTasks[last].MemNeed {
				taskIdx := []int{}
				//todo:通过二分查找优化
				for i := 0; i < len(s.unAllocTasks) && len(w.tasks) < MaxTaskCount; i++ {
					v := s.unAllocTasks[i]
					if w.memory >= v.MemNeed {
						taskIdx = append(taskIdx, i)
						logger.Sugar().Debugf("dispatchJob %s to worker:%s mem before:%d mem after:%d task mem:%d", v.Id, w.workerID, w.memory, w.memory-v.MemNeed, v.MemNeed)
						w.memory -= v.MemNeed
						w.tasks[v.Id] = v
						v.WorkerID = w.workerID
						v.save(s.db)
						v.deadline = time.Now().Add(time.Second * taskTimeout)
						s.doing[v.Id] = v
						w.dispatchJob(v, s.cfg.ThreadReserved)
					}
				}

				if len(taskIdx) > 0 {
					c := len(s.unAllocTasks) - 1
					for _, v := range taskIdx {
						s.unAllocTasks[c], s.unAllocTasks[v] = s.unAllocTasks[v], s.unAllocTasks[c]
						c--
					}
					s.unAllocTasks = s.unAllocTasks[:len(s.unAllocTasks)-len(taskIdx)]
					sort.Slice(s.unAllocTasks, func(i, j int) bool {
						return s.unAllocTasks[i].less(s.unAllocTasks[j])
					})
				}
			}
		}
	}

	if len(w.tasks) >= MaxTaskCount {
		w.inAvailable = false
	} else if !w.inAvailable {
		w.inAvailable = true
		s.availableWorkers = append(s.availableWorkers, w)
		if dosort {
			sort.Slice(s.availableWorkers, func(i, j int) bool {
				return s.availableWorkers[i].memory < s.availableWorkers[j].memory
			})
		}
	}
}

func (s *sche) tryDispatchJob() {
	if atomic.LoadInt32(&s.dispatchFlag) == 1 {
		return
	}
	availableWorkers := s.availableWorkers
	s.availableWorkers = []*worker{}
	for _, w := range availableWorkers {
		w.inAvailable = false
		s.onWorkerAvaliable(w, false)
	}
	sort.Slice(s.availableWorkers, func(i, j int) bool {
		return s.availableWorkers[i].memory < s.availableWorkers[j].memory
	})
}

func (s *sche) getTaskCount() (unalloc int, doing int, finish int, total int) {
	ch := make(chan struct{})
	s.processQueue <- func() {
		unalloc = len(s.unAllocTasks)
		total = len(s.tasks)
		doing = len(s.doing)
		finish = total - doing - unalloc
		close(ch)
	}
	<-ch
	return
}

func (s *sche) getWorkers() []listEntry {
	ret := make(chan []listEntry)
	s.processQueue <- func() {
		var workers []listEntry
		for _, v := range s.workers {
			e := listEntry{
				worker: v.workerID,
				memory: v.memory,
				core:   v.threadcount,
			}
			for _, vv := range v.tasks {
				e.tasks = append(e.tasks, *vv)
			}
			workers = append(workers, e)
		}
		ret <- workers
	}
	return <-ret
}
