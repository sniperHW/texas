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

type task struct {
	id         string
	memNeed    int
	cfgPath    string
	resultPath string
	ok         bool
}

func (t *task) less(o *task) bool {
	if t.memNeed < o.memNeed {
		return true
	} else if t.memNeed == o.memNeed {
		return t.id < o.id
	} else {
		return false
	}
}

/*
 *  一个作业，最多由两个task构成
 *  job是基本的分配单位
 */
type job struct {
	workerID string
	tasks    []*task
	group    *taskGroup
	deadline time.Time
}

func (j *job) memNeed() (m int) {
	for _, v := range j.tasks {
		m += v.memNeed
	}
	return m
}

func (j *job) id() string {
	return j.tasks[0].id
}

type jobJson struct {
	Worker string
	Ok     bool
	Tasks  []string
}

func (j *job) save(db *bolt.DB, finish bool) {
	jobSt := jobJson{
		Worker: j.workerID,
		Ok:     finish,
	}
	for _, v := range j.tasks {
		jobSt.Tasks = append(jobSt.Tasks, v.id)
	}

	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket([]byte(j.group.filepath))
		if jobSt.Worker == "" {
			err = bucket.Delete([]byte(jobSt.Tasks[0]))
		} else {
			jsonBytes, _ := json.Marshal(&jobSt)
			err = bucket.Put([]byte(jobSt.Tasks[0]), jsonBytes)
		}
		return err
	})

	if err != nil {
		logger.Sugar().Error(err)
	}
}

type worker struct {
	workerID string
	memory   int
	job      *job //当前正在执行的job
	socket   *netgo.AsynSocket
}

func (w *worker) dispatchJob(job *job) {
	msg := &proto.DispatchJob{}
	for _, v := range job.tasks {
		msg.Tasks = append(msg.Tasks, proto.Task{
			TaskID:     v.id,
			CfgPath:    v.cfgPath,
			ResultPath: v.resultPath,
		})
	}
	w.socket.Send(msg)
}

type taskGroup struct {
	filepath     string
	tasks        map[string]*task
	unAllocTasks []*task //尚未分配执行的任务，按memNeed升序排列
}

type sche struct {
	workers      map[string]*worker
	taskGroups   map[string]*taskGroup
	doing        map[string]*job //求解中的job
	freeWorkers  []*worker       //根据memory按升序排列
	processQueue chan func()
	die          chan struct{}
	stopc        chan struct{}
	cfg          *Config
	db           *bolt.DB
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
				id:         fields[0],
				cfgPath:    fields[2],
				resultPath: fields[4],
			}

			t.memNeed, err = strconv.Atoi(fields[3])

			if err != nil {
				return fmt.Errorf("(2)invaild task:%s error:%v", taskStr, err)
			}

			g.tasks[t.id] = t
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

	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(g.filepath))
		b.ForEach(func(k, v []byte) error {
			var jobSt jobJson
			err := json.Unmarshal(v, &jobSt)
			if err != nil {
				return err
			}

			logger.Sugar().Debug(jobSt)

			if jobSt.Ok {
				for _, v := range jobSt.Tasks {
					t := g.tasks[v]
					t.ok = true
				}
			} else {
				j := job{
					workerID: jobSt.Worker,
					group:    g,
					deadline: time.Now().Add(time.Second * 30),
				}

				for _, v := range jobSt.Tasks {
					j.tasks = append(j.tasks, g.tasks[v])
				}

				s.doing[j.id()] = &j
			}
			return nil
		})
		return nil
	})

	if err == nil {
		for _, v := range g.tasks {
			if !v.ok {
				g.unAllocTasks = append(g.unAllocTasks, v)
			}
		}

		sort.Slice(g.unAllocTasks, func(i, j int) bool {
			return g.unAllocTasks[i].less(g.unAllocTasks[j])
		})
	}

	logger.Sugar().Debugf("unAllocTasks:%d", len(g.unAllocTasks))

	return err
}

func (s *sche) addTaskFile(file string) {
	group := &taskGroup{
		filepath: file,
		tasks:    map[string]*task{},
	}

	if err := group.loadTaskFromFile(s); err == nil {

		for _, vv := range group.tasks {
			group.unAllocTasks = append(group.unAllocTasks, vv)
		}

		sort.Slice(group.unAllocTasks, func(i, j int) bool {
			return group.unAllocTasks[i].less(group.unAllocTasks[j])
			//return group.unAllocTasks[i].memNeed < group.unAllocTasks[j].memNeed
		})

		s.taskGroups[group.filepath] = group

		s.tryDispatchJob(group)

	} else {
		logger.Sugar().Errorf("loadTaskFromFile(%s)  error:%v", file, err)
	}
}

func (s *sche) removeTaskFile(file string) {
	delete(s.taskGroups, file)
	for _, vv := range s.doing {
		if vv.group.filepath == file {
			delete(s.doing, vv.id())
			if worker := s.workers[vv.workerID]; worker != nil {
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
				tasks:    map[string]*task{},
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

			//logger.Sugar().Debugf("on new worker(%v %v)", w.workerID, w.memory)

			socket.SetUserData(w)
			s.workers[w.workerID] = w

			socket.SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
				s.processQueue <- func() {
					delete(s.workers, w.workerID)
					//logger.Sugar().Debugf("worker:%s disconnected2", w.workerID)
					if w.job == nil {
						for i, v := range s.freeWorkers {
							if v == w {
								i = i + 1
								for ; i < len(s.freeWorkers); i++ {
									s.freeWorkers[i-1] = s.freeWorkers[i]
								}
								s.freeWorkers = s.freeWorkers[:len(s.freeWorkers)-1]
							}
						}
					}
				}
			})

			if h.JobID == "" {
				for _, v := range s.doing {
					if v.workerID == w.workerID {
						/*
						 * worker在求解过程中进程崩溃，重启后重新连上sche
						 */
						w.job = v
						v.deadline = time.Now().Add(time.Second * 30)
						w.dispatchJob(v)
						return
					}
				}
				s.addFreeWorker(w)
			} else {
				if job := s.doing[h.JobID]; job != nil {
					job.deadline = time.Now().Add(time.Second * 30)
				} else {
					w.socket.Send(&proto.CancelJob{})
				}
			}
		}
	} else {
		if w.job != nil {
			if h.JobID == "" {
				w.job = nil
				s.addFreeWorker(w)
			} else {
				w.job.deadline = time.Now().Add(time.Second * 30)
			}
		}
	}
}

func (s *sche) onCommitJobResult(socket *netgo.AsynSocket, commit *proto.CommitJobResult) {
	logger.Sugar().Debugf("onCommitJobResult %v", commit.JobID)
	if w, _ := socket.GetUserData().(*worker); w != nil {
		j := s.doing[commit.JobID]
		if j == w.job {
			logger.Sugar().Debugf("onCommitJobResult1 %v", commit.JobID)
			for _, v := range j.tasks {
				if err := os.Rename(v.resultPath+".tmp", v.resultPath); err != nil {
					logger.Sugar().Errorf("rename file:%s error:%v", v.resultPath, err)
				}
			}
			j.save(s.db, true)
			delete(s.doing, j.tasks[0].id)
			w.socket.Send(&proto.AcceptJobResult{
				JobID: commit.JobID,
			})
		} else {
			w.socket.Send(&proto.CancelJob{})
		}
	}
}

func (s *sche) dispatchJob(job *job) {
	memNeed := job.memNeed()
	for i, v := range s.freeWorkers {
		if v.memory >= memNeed {
			w := v
			i = i + 1
			for ; i < len(s.freeWorkers); i++ {
				s.freeWorkers[i-1] = s.freeWorkers[i]
			}
			s.freeWorkers = s.freeWorkers[:len(s.freeWorkers)-1]

			job.workerID = w.workerID

			//s.storage.WriteString(job.toString())
			//s.storage.Sync()

			job.save(s.db, false)

			job.deadline = time.Now().Add(time.Second * 30)
			w.job = job
			s.doing[job.id()] = job
			w.dispatchJob(job)
			return
		}
	}

	//没有合适的worker,将job添加到jobQueue
	job.workerID = ""
	delete(s.doing, job.id())

	job.save(s.db, false)

	//s.storage.WriteString(job.toString())
	//s.storage.Sync()

	job.group.unAllocTasks = append(job.group.unAllocTasks, job.tasks...)

	sort.Slice(job.group.unAllocTasks, func(i, j int) bool {
		return job.group.unAllocTasks[i].less(job.group.unAllocTasks[j])
		//return job.group.unAllocTasks[i].memNeed < job.group.unAllocTasks[j].memNeed
	})

}

func (s *sche) start() {
	ticker := time.NewTicker(time.Second)

	go func() {
		for range ticker.C {
			s.processQueue <- func() {
				//从新分配超时任务
				var timeout []*job
				now := time.Now()
				for n, v := range s.doing {
					if now.After(v.deadline) {
						timeout = append(timeout, v)
						delete(s.doing, n)
						v.workerID = ""
					}
				}

				for _, v := range timeout {
					logger.Sugar().Debugln(v.workerID)
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

func (s *sche) addFreeWorker(w *worker) {

	logger.Sugar().Debugf("addFreeWorker:%s", w.workerID)

	memory := w.memory

	for _, g := range s.taskGroups {
		j := job{}
		for i := 0; len(j.tasks) < 2 && i < len(g.unAllocTasks); i++ {
			t := g.unAllocTasks[i]
			if t.memNeed > memory {
				break
			}
			memory -= t.memNeed
			j.tasks = append(j.tasks, t)
		}
		taskLen := len(j.tasks)
		if taskLen > 0 {
			for i := taskLen; i < len(g.unAllocTasks); i++ {
				g.unAllocTasks[i-taskLen] = g.unAllocTasks[i]
			}
			g.unAllocTasks = g.unAllocTasks[:len(g.unAllocTasks)-taskLen]

			j.workerID = w.workerID
			j.group = g
			j.save(s.db, false)
			//s.storage.WriteString(j.toString())
			//s.storage.Sync()
			j.deadline = time.Now().Add(time.Second * 30)
			s.doing[j.id()] = &j
			w.job = &j
			w.dispatchJob(&j)
			return
		}
	}

	s.freeWorkers = append(s.freeWorkers, w)

	sort.Slice(s.freeWorkers, func(i, j int) bool {
		return s.freeWorkers[i].memory < s.freeWorkers[j].memory
	})
}

func (s *sche) tryDispatchJob(group *taskGroup) {
	workerCount := len(s.freeWorkers)
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

}
