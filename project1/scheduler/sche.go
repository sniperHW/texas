package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

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

func (j *job) toString() string {
	if len(j.tasks) == 1 {
		return fmt.Sprintf("%s,%s,%s\n", j.group.filepath, j.workerID, j.tasks[0].id)
	} else {
		return fmt.Sprintf("%s,%s,%s,%s\n", j.group.filepath, j.workerID, j.tasks[0].id, j.tasks[1].id)
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
	storage      *os.File
	cfg          *Config
}

func (g *taskGroup) loadTaskFromFile() error {
	logger.Sugar().Debugf("load %s", g.filepath)

	var f *os.File
	var err error
	f, err = os.Open(g.filepath)
	if err != nil {
		return err
	}

	var record []byte
	b := make([]byte, 1)
	for {
		_, e := f.Read(b)
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		} else {
			switch b[0] {
			case '\r':
			case '\n':
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

				//检查result文件是否存在
				ff, e := os.Open(t.resultPath)
				if e == nil {
					ff.Close()
					t.ok = true
				} else if !os.IsNotExist(e) {
					return fmt.Errorf("open result file:%s error:%v", t.resultPath, e)
				}
				g.tasks[t.id] = t
			default:
				record = append(record, b[0])
			}
		}
	}

	return err
}

func (s *sche) addTaskFile(file string) {
	group := &taskGroup{
		filepath: file,
		tasks:    map[string]*task{},
	}

	if err := group.loadTaskFromFile(); err == nil {

		for _, vv := range group.tasks {
			group.unAllocTasks = append(group.unAllocTasks, vv)
		}

		sort.Slice(group.unAllocTasks, func(i, j int) bool {
			return group.unAllocTasks[i].memNeed < group.unAllocTasks[j].memNeed
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
}

func (s *sche) init() error {
	doingTask := map[string]map[string]*task{}
	err := filepath.Walk(s.cfg.TaskCfg, func(filePath string, f os.FileInfo, _ error) error {
		if f != nil && !f.IsDir() {
			group := &taskGroup{
				filepath: filePath,
				tasks:    map[string]*task{},
			}

			doingTask[group.filepath] = map[string]*task{}
			if err := group.loadTaskFromFile(); err == nil {
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

	/*
	 *  storage:filename,workerID,task1,task2
	 */

	var f *os.File
	f, err = os.Open(s.cfg.Storage)
	if err == nil {
		var record []byte
		b := make([]byte, 1)
		for {
			_, e := f.Read(b)
			if e != nil {
				if e != io.EOF {
					return e
				}
				break
			} else {
				switch b[0] {
				case '\r':
				case '\n':
					jobStr := string(record)
					record = record[:0]
					fields := strings.Split(jobStr, ",")
					//如果找不到说明整个文件都已经被删除，里面的task也就被丢弃
					if g, ok := s.taskGroups[fields[0]]; ok {
						doingTaskMap := doingTask[g.filepath]
						workerID := fields[1]
						if len(fields) > 3 {
							task1 := g.tasks[fields[2]]
							task2 := g.tasks[fields[3]]
							if task1 == nil || task2 == nil {
								return fmt.Errorf("invaild job:%s", jobStr)
							}

							if workerID == "" {
								delete(s.doing, task1.id)
								delete(doingTaskMap, task1.id)
								delete(doingTaskMap, task2.id)
							} else {
								if !task1.ok || !task2.ok {
									if !task1.ok {
										doingTaskMap[task1.id] = task1
									}

									if !task2.ok {
										doingTaskMap[task2.id] = task2
									}

									s.doing[task1.id] = &job{
										workerID: fields[1],
										group:    g,
										tasks:    []*task{task1, task2},
									}
								}
							}

						} else {
							task1 := g.tasks[fields[2]]
							if task1 == nil {
								return fmt.Errorf("invaild job:%s", jobStr)
							}

							if workerID == "" {
								delete(s.doing, task1.id)
								delete(doingTaskMap, task1.id)
							} else {
								if !task1.ok {
									doingTaskMap[task1.id] = task1
									s.doing[task1.id] = &job{
										workerID: fields[1],
										group:    g,
										tasks:    []*task{task1},
									}
								}
							}
						}
					}
				default:
					record = append(record, b[0])
				}
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	for _, g := range s.taskGroups {
		doingTaskMap := doingTask[g.filepath]
		for _, t := range g.tasks {
			if !t.ok && doingTaskMap[t.id] == nil {
				g.unAllocTasks = append(g.unAllocTasks, t)
			}
		}
		sort.Slice(g.unAllocTasks, func(i, j int) bool {
			return g.unAllocTasks[i].memNeed < g.unAllocTasks[j].memNeed
		})
	}

	f.Close()

	tmpName := s.cfg.Storage + ".tmp"

	f, err = os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	for _, j := range s.doing {
		if len(j.tasks) == 1 {
			f.WriteString(fmt.Sprintf("%s,%s,%s\n", j.group.filepath, j.workerID, j.tasks[0].id))
		} else {
			f.WriteString(fmt.Sprintf("%s,%s,%s,%s\n", j.group.filepath, j.workerID, j.tasks[0].id, j.tasks[1].id))
		}
	}
	if err = f.Close(); err != nil {
		return err
	}

	//替换原文件
	if err = os.Rename(tmpName, s.cfg.Storage); err != nil {
		return err
	}

	s.storage, err = os.OpenFile(s.cfg.Storage, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

			s.storage.WriteString(job.toString())
			s.storage.Sync()

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
	s.storage.WriteString(job.toString())
	s.storage.Sync()

	job.group.unAllocTasks = append(job.group.unAllocTasks, job.tasks...)

	sort.Slice(job.group.unAllocTasks, func(i, j int) bool {
		return job.group.unAllocTasks[i].memNeed < job.group.unAllocTasks[j].memNeed
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

	for task := range s.processQueue {
		task()
	}
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
			s.storage.WriteString(j.toString())
			s.storage.Sync()
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

		s.storage.WriteString(job.toString())
		s.storage.Sync()
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

/*
func (s *sche) reload() error {
	var olds []string
	for _, v := range s.taskGroups {
		olds = append(olds, v.filepath)
	}

	sort.Slice(olds, func(i, j int) bool {
		return olds[i] < olds[j]
	})

	var news []string
	err := filepath.Walk(s.cfg.CfgPath, func(filePath string, f os.FileInfo, _ error) error {
		if f != nil && !f.IsDir() {
			news = append(news, path.Join(filePath, f.Name()))
		}
		return nil
	})

	if err != nil {
		return err
	}

	sort.Slice(news, func(i, j int) bool {
		return news[i] < news[j]
	})

	var adds []string    //新增的配置文件
	var removes []string //被删除的配置文件

	i := 0
	j := 0

	for i < len(news) && j < len(olds) {
		nodej := olds[j]
		nodei := news[i]

		if nodei == nodej {
			i++
			j++
		} else if nodei > nodej {
			//local  1 2 3 4 5 6
			//update 1 2 4 5 6
			//移除节点
			removes = append(removes, nodej)
			j++
		} else {
			//local  1 2 4 5 6
			//update 1 2 3 4 5 6
			//添加节点
			adds = append(adds, nodei)
			i++
		}
	}

	adds = append(adds, news[i:]...)

	removes = append(removes, olds[j:]...)

	for _, v := range removes {
		delete(s.taskGroups, v)
		for _, vv := range s.doing {
			if vv.group.filepath == v {
				delete(s.doing, vv.id())
				if worker := s.workers[vv.workerID]; worker != nil {
					worker.socket.Send(&proto.CancelJob{})
				}
			}
		}
	}

	for _, v := range adds {
		group := &taskGroup{
			filepath: v,
			tasks:    map[string]*task{},
		}

		if err := group.loadTaskFromFile(); err == nil {

			for _, vv := range group.tasks {
				group.unAllocTasks = append(group.unAllocTasks, vv)
			}

			sort.Slice(group.unAllocTasks, func(i, j int) bool {
				return group.unAllocTasks[i].memNeed < group.unAllocTasks[j].memNeed
			})

			s.taskGroups[group.filepath] = group

			s.tryDispatchJob(group)

		} else {
			return err
		}
	}

	return nil
}
*/
