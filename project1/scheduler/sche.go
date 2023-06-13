package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/fsnotify/fsnotify"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
)

const MaxTaskCount = 2

type task struct {
	Id         string
	MemNeed    int
	CfgPath    string
	ResultPath string
	WorkerID   string
	Ok         bool
	group      *taskGroup
	deadline   time.Time
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

func (t *task) save(db *bolt.DB, finish bool) {
	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket([]byte(t.group.filepath))
		if t.WorkerID == "" {
			err = bucket.Delete([]byte(t.Id))
		} else {
			jsonBytes, _ := json.Marshal(t)
			err = bucket.Put([]byte(t.Id), jsonBytes)
		}
		return err
	})

	if err != nil {
		logger.Sugar().Error(err)
	}
}

type worker struct {
	workerID    string
	memory      int
	tasks       map[string]*task
	socket      *netgo.AsynSocket
	inAvailable bool
}

func (w *worker) dispatchJob(task *task) {
	msg := &proto.DispatchJob{
		Task: proto.Task{
			TaskID:     task.Id,
			CfgPath:    task.CfgPath,
			ResultPath: task.ResultPath,
		},
	}
	w.socket.Send(msg)
}

type taskGroup struct {
	filepath string
}

type sche struct {
	workers          map[string]*worker
	taskGroups       map[string]*taskGroup
	doing            map[string]*task //求解中的task
	tasks            map[string]*task
	unAllocTasks     []*task   //尚未分配执行的任务，按memNeed升序排列
	availableWorkers []*worker //根据memory按升序排列
	processQueue     chan func()
	die              chan struct{}
	stopc            chan struct{}
	cfg              *Config
	db               *bolt.DB
	pause            bool
}

func (g *taskGroup) loadTaskFromFile(s *sche) error {
	logger.Sugar().Debugf("load %s", g.filepath)

	var f *os.File
	var err error

	err = s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(g.filepath))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("could not set up buckets, %v", err)
	}

	f, err = os.Open(g.filepath)
	if err != nil {
		return err
	}

	var record []byte

	process := func() error {
		if len(record) > 0 {
			taskStr := string(record)
			record = record[:0]
			fields := strings.Split(taskStr, "\t")
			if len(fields) != 5 {
				return fmt.Errorf("(1)invaild task:%s", taskStr)
			}

			t := &task{
				Id:         fields[0],
				CfgPath:    fields[2],
				ResultPath: fields[4],
				group:      g,
			}

			t.MemNeed, err = strconv.Atoi(fields[3])

			if err != nil {
				return fmt.Errorf("(2)invaild task:%s error:%v", taskStr, err)
			}

			s.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(g.filepath))
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

			if !t.Ok {
				if t.WorkerID != "" {
					t.deadline = time.Now().Add(time.Second * 30)
					s.doing[t.Id] = t
				} else {
					s.unAllocTasks = append(s.unAllocTasks, t)
				}
			}

			s.tasks[t.Id] = t

		}
		return nil
	}

	b := make([]byte, 1)
	for {
		_, err = f.Read(b)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				if err = process(); err != nil {
					return err
				}
				break
			}
		} else {
			switch b[0] {
			case '\r':
			case '\n':
				if err = process(); err != nil {
					return err
				}
			default:
				record = append(record, b[0])
			}
		}
	}

	return err
}

func (s *sche) Pause() {
	s.pause = true
}

func (s *sche) Resume() {
	s.pause = false
	s.tryDispatchJob()
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
	delete(s.taskGroups, file)
	for _, vv := range s.doing {
		if vv.group.filepath == file {
			delete(s.doing, vv.Id)
			if worker := s.workers[vv.WorkerID]; worker != nil {
				worker.socket.Send(&proto.CancelJob{})
			}
		}
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(file))
		if err != nil {
			return fmt.Errorf("could not delete root bucket: %v", err)
		}
		return nil
	})

	if err != nil {
		logger.Sugar().Error(err)
	}

}

func (s *sche) init() error {
	err := filepath.Walk(s.cfg.TaskCfg, func(filePath string, f os.FileInfo, _ error) error {
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
							}
						}
					}

					if ev.Op&fsnotify.Remove == fsnotify.Remove {
						fi, err := os.Stat(ev.Name)
						if err == nil && !fi.IsDir() {
							s.processQueue <- func() {
								s.removeTaskFile(ev.Name)
							}
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

	return nil
}

func (s *sche) onWorkerHeartBeat(socket *netgo.AsynSocket, h *proto.WorkerHeartBeat) {
	w, _ := socket.GetUserData().(*worker)
	if w == nil {
		w = s.workers[h.WorkerID]
		if w != nil {
			socket.Close(errors.New("duplicate worker"))
			return
		} else {
			w = &worker{
				workerID: h.WorkerID,
				memory:   int(h.Memory),
				socket:   socket,
			}

			socket.SetUserData(w)
			s.workers[w.workerID] = w

			socket.SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
				s.processQueue <- func() {
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

			if len(h.Tasks) > 0 {
				for _, v := range h.Tasks {
					if task := s.doing[v]; task != nil && task.WorkerID == v {
						w.tasks[task.Id] = task
						w.memory -= task.MemNeed
						task.deadline = time.Now().Add(time.Second * 30)
					} else {
						w.socket.Send(&proto.CancelJob{TaskID: v})
					}
				}
			} else {
				for _, v := range s.doing {
					if v.WorkerID == w.workerID {
						/*
						 * worker在求解过程中进程崩溃，重启后重新连上sche
						 */
						w.tasks[v.Id] = v
						w.memory -= v.MemNeed
						v.deadline = time.Now().Add(time.Second * 30)
						w.dispatchJob(v)
					}
				}
			}

			if w.memory > 0 && len(w.tasks) != MaxTaskCount {
				s.onWorkerAvaliable(w, true)
			}
		}
	} else {
		for _, v := range w.tasks {
			find := false
			for _, vv := range h.Tasks {
				if v.Id == vv {
					find = true
					v.deadline = time.Now().Add(time.Second * 30)
					break
				}
			}

			if !find {
				delete(w.tasks, v.Id)
				w.memory += v.MemNeed
			}
		}
		s.onWorkerAvaliable(w, true)
	}
}

func (s *sche) onCommitJobResult(socket *netgo.AsynSocket, commit *proto.CommitJobResult) {
	logger.Sugar().Debugf("onCommitJobResult %v", commit.TaskID)
	if w, _ := socket.GetUserData().(*worker); w != nil {
		for _, v := range w.tasks {
			if commit.TaskID == v.Id {
				v.save(s.db, true)
				delete(s.doing, v.Id)
				w.socket.Send(&proto.AcceptJobResult{
					TaskID: commit.TaskID,
				})
				return
			}
		}
		w.socket.Send(&proto.CancelJob{TaskID: commit.TaskID})
	}
}

func (s *sche) dispatchJob(task *task) {
	if !s.pause {
		//寻找一个worker将task分配出去，如果没有合适的worker将task放回到unAllocTasks
		for i, v := range s.availableWorkers {
			if v.memory >= task.MemNeed {
				w := v

				task.WorkerID = w.workerID

				task.save(s.db, false)

				task.deadline = time.Now().Add(time.Second * 30)

				w.tasks[task.Id] = task
				w.memory -= task.MemNeed

				//如果memory不足或已经运行了MaxTaskCount数量的任务，将worker从availableWorkers移除
				if w.memory == 0 || len(w.tasks) == MaxTaskCount {
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
				w.dispatchJob(task)
				return
			}
		}
	}

	//没有合适的worker,将job添加到jobQueue
	task.WorkerID = ""
	delete(s.doing, task.Id)

	task.save(s.db, false)

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
					logger.Sugar().Debugln(v.WorkerID)
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
	if !s.pause {

		taskIdx := []int{}

		//todo:通过二分查找优化
		for i, v := range s.unAllocTasks {
			if w.memory >= v.MemNeed {
				taskIdx = append(taskIdx, i)
				w.memory -= v.MemNeed
				w.tasks[v.Id] = v
				v.WorkerID = w.workerID
				v.save(s.db, false)
				v.deadline = time.Now().Add(time.Second * 30)
				s.doing[v.Id] = v
				w.dispatchJob(v)
				if len(w.tasks) == MaxTaskCount || w.memory == 0 {
					break
				}
			}
		}

		if len(taskIdx) > 0 {

			c := len(s.unAllocTasks) - 1
			for _, v := range taskIdx {
				s.unAllocTasks[c], s.unAllocTasks[v] = s.unAllocTasks[v], s.unAllocTasks[c]
				c--
			}

			sort.Slice(s.unAllocTasks, func(i, j int) bool {
				return s.unAllocTasks[i].less(s.unAllocTasks[j])
			})
		}
	}

	if len(w.tasks) == MaxTaskCount || w.memory == 0 {
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
	if s.pause {
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

	/*workerCount := len(s.freeWorkers)
	taskCount := len(group.unAllocTasks)
	//workerRemIdx := -1
	markRemove := make([]bool, len(s.freeWorkers))
	workerIdx := 0
	taskIdx := 0
	for workerCount > 0 && taskCount > 0 {
		lower := group.unAllocTasks[taskIdx].memNeed

		upper := lower
		if taskCount > 1 {
			upper += group.unAllocTasks[taskIdx+1].memNeed
		}

		lowerIdx := -1 //满足lower内存要求的第一个worker在freeWorkers中的下标
		upperIdx := -1 //满足upper内存要求的第一个worker在freeWorkers中的下标
		for i := workerIdx; i < len(s.freeWorkers); i++ {
			v := s.freeWorkers[i]
			if lowerIdx == -1 && v.memory >= lower {
				lowerIdx = i
			}

			if upperIdx == -1 && v.memory >= upper {
				upperIdx = i
			}

			if lowerIdx >= 0 && upperIdx >= 0 {
				break
			}
		}

		if lowerIdx == -1 {
			break
		}

		idx := lowerIdx

		tasks := []*task{group.unAllocTasks[taskIdx]}
		taskCount--
		taskIdx++
		if lower != upper && upperIdx >= 0 && s.freeWorkers[upperIdx].memory >= upper {
			tasks = append(tasks, group.unAllocTasks[taskIdx])
			taskCount--
			taskIdx++
			idx = upperIdx
		}

		workerCount--

		worker := s.freeWorkers[idx]
		workerIdx = idx + 1

		markRemove[idx] = true

		job := job{
			workerID: worker.workerID,
			tasks:    tasks,
			group:    group,
		}

		//s.storage.WriteString(job.toString())
		//s.storage.Sync()
		job.save(s.db, false)

		job.deadline = time.Now().Add(time.Second * 30)
		s.doing[job.id()] = &job
		worker.dispatchJob(&job)
	}

	//将removeWorker中标记的worker移除
	var freeWorkers []*worker
	for i, v := range s.freeWorkers {
		if !markRemove[i] {
			freeWorkers = append(freeWorkers, v)
		}
	}
	s.freeWorkers = freeWorkers

	//将taskIdx之前的task移除
	for i := taskIdx; i < len(group.unAllocTasks); i++ {
		group.unAllocTasks[i-taskIdx] = group.unAllocTasks[i]
	}
	group.unAllocTasks = group.unAllocTasks[:len(group.unAllocTasks)-taskIdx]
	*/
}
