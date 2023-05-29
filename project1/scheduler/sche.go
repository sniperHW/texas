package main

import (
	"container/list"
	"errors"
	"sort"
	"time"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
)

type Job struct {
	JobID      string
	WorkerID   string
	MemNeed    uint32
	CfgPath    string
	ResultPath string
	Finish     bool
	deadline   time.Time
}

type worker struct {
	workerID string
	memory   uint32
	job      *Job //当前正在执行的job
	socket   *netgo.AsynSocket
}

func (w *worker) dispatchJob(job *Job) {
	w.socket.Send(&proto.DispatchJob{
		JobID:   job.JobID,
		CfgPath: job.CfgPath,
	})
}

type sche struct {
	jobs         map[string]*Job
	jobFinish    map[string]*Job //已经有结果的任务
	jobQueue     *list.List      //待分配work的job队列
	jobDoing     map[string]*Job //求解中的job
	freeWorkers  []*worker       //根据memory按升序排列
	workers      map[string]*worker
	processQueue chan func()
	storage      storage
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
				memory:   uint32(h.Memory),
				socket:   socket,
			}

			logger.Sugar().Debugf("on new worker(%v %v)", w.workerID, w.memory)

			socket.SetUserData(w)
			s.workers[w.workerID] = w

			socket.SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
				s.processQueue <- func() {
					delete(s.workers, w.workerID)
					logger.Sugar().Debugf("worker:%s disconnected2", w.workerID)
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
				for _, v := range s.jobDoing {
					if v.WorkerID == w.workerID {
						w.job = v
						v.deadline = time.Now().Add(time.Second * 30)
						w.dispatchJob(v)
						return
					}
				}
				s.addFreeWorker(w)
			} else {
				w.socket.Send(&proto.CancelJob{})
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
	logger.Sugar().Debugf("onCommitJobResult1")
	if w, _ := socket.GetUserData().(*worker); w != nil {
		logger.Sugar().Debugf("onCommitJobResult2")
		j := s.jobDoing[commit.JobID]
		if j == w.job {
			j.Finish = true
			//先保存
			s.storage.Save(j)
			s.jobFinish[j.JobID] = j
			delete(s.jobDoing, commit.JobID)
			w.socket.Send(&proto.AcceptJobResult{
				JobID: commit.JobID,
			})
			logger.Sugar().Debugf("job:%s finish", j.JobID)
		} else {
			w.socket.Send(&proto.CancelJob{})
		}
	}
}

// 将job分发给worker,如果没有合适的worker，将job追加到队列中
func (s *sche) dispatchJob(job *Job) {
	for i, v := range s.freeWorkers {
		if v.memory >= job.MemNeed {
			w := v
			i = i + 1
			for ; i < len(s.freeWorkers); i++ {
				s.freeWorkers[i-1] = s.freeWorkers[i]
			}
			s.freeWorkers = s.freeWorkers[:len(s.freeWorkers)-1]
			job.WorkerID = w.workerID

			s.storage.Save(job)

			job.deadline = time.Now().Add(time.Second * 30)
			w.job = job
			s.jobDoing[job.JobID] = job
			w.dispatchJob(job)

			logger.Sugar().Debugf("dispatch job:%s to %s", job.JobID, w.workerID)

			return
		}
	}

	//没有合适的worker,将job添加到jobQueue
	job.WorkerID = ""
	s.storage.Save(job)

	logger.Sugar().Debugf("job:%s queueing", job.JobID)

	s.jobQueue.PushBack(job)
}

func (s *sche) onNewJob(job *Job) error {
	j := s.jobs[job.JobID]
	if j != nil {
		if j.CfgPath != job.CfgPath {
			return errors.New("重复的任务")
		}

		if j.MemNeed != job.MemNeed {
			return errors.New("重复的任务")
		}

		if j.ResultPath != job.ResultPath {
			return errors.New("重复的任务")
		}
		return nil
	}

	s.jobs[job.JobID] = job

	s.dispatchJob(job)

	return nil
}

func (s *sche) addFreeWorker(w *worker) {

	logger.Sugar().Debugf("addFreeWorker:%s", w.workerID)

	for e := s.jobQueue.Front(); e != nil; e = e.Next() {
		job := e.Value.(*Job)
		if job.MemNeed <= w.memory {
			s.jobQueue.Remove(e)

			job.WorkerID = w.workerID
			job.deadline = time.Now().Add(time.Second * 30)
			w.job = job
			s.jobDoing[job.JobID] = job
			s.storage.Save(job)
			w.dispatchJob(job)
			return
		}
	}

	s.freeWorkers = append(s.freeWorkers, w)

	sort.Slice(s.freeWorkers, func(i, j int) bool {
		return s.freeWorkers[i].memory < s.freeWorkers[j].memory
	})
}

func (s *sche) start() {
	ticker := time.NewTicker(time.Second)

	go func() {
		for range ticker.C {
			s.processQueue <- func() {
				//从新分配超时任务
				var timeout []*Job
				now := time.Now()
				for _, v := range s.jobDoing {
					if now.After(v.deadline) {
						timeout = append(timeout, v)
						delete(s.jobDoing, v.JobID)
						v.WorkerID = ""
					}
				}

				for _, v := range timeout {
					s.dispatchJob(v)
				}
			}
		}
	}()

	for task := range s.processQueue {
		task()
	}
}
