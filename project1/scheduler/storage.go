package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type storage interface {
	Load() (map[string]*Job, map[string]*Job, map[string]*Job, error) //finish,doing,queueing
	Save(*Job)
}

type fileStorage struct {
	file *os.File
	path string
}

func NewFileStorage(path string) (*fileStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &fileStorage{
		file: f,
		path: path,
	}, nil
}

func (s *fileStorage) Save(job *Job) {
	_, err := s.file.WriteString(fmt.Sprintf("%s,%s,%d,%s,%s,%v\n", job.JobID, job.WorkerID, job.MemNeed, job.CfgPath, job.ResultPath, job.Finish))
	if err != nil {
		panic(err)
	}
	err = s.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (s *fileStorage) Load() (finish map[string]*Job, doing map[string]*Job, queueing map[string]*Job, err error) {
	var f *os.File
	f, err = os.Open(s.path)
	if err != nil {
		return
	}

	finish = map[string]*Job{}
	doing = map[string]*Job{}
	queueing = map[string]*Job{}

	var record []byte
	b := make([]byte, 1)
	for {
		_, e := f.Read(b)
		if e != nil {
			if e == io.EOF {
				if len(record) > 0 && record[len(record)-1] != byte('\n') {
					err = errors.New("文件不完整")
				}
				break
			} else {
				err = e
				break
			}
		} else {
			record = append(record, b[0])
			if b[0] == byte('\n') {
				job := &Job{}
				//fmt.Sscanf(string(record), "%s,%s,%d,%s,%s,%v\n", &job.JobID, &job.WorkerID, &job.MemNeed, &job.CfgPath, &job.ResultPath, &job.Finish)
				fields := strings.Split(string(record), ",")
				if len(fields) != 6 {
					panic("文件内容错误")
				}

				job.JobID = fields[0]
				job.WorkerID = fields[1]
				MemNeed, _ := strconv.ParseUint(fields[2], 10, 32)
				job.MemNeed = uint32(MemNeed)
				job.CfgPath = fields[3]
				job.ResultPath = fields[4]
				job.Finish, _ = strconv.ParseBool(fields[5])

				record = record[:0]

				if job.Finish {
					finish[job.JobID] = job
					delete(doing, job.JobID)
				} else if job.WorkerID == "" {
					queueing[job.JobID] = job
					delete(doing, job.JobID)
				} else {
					doing[job.JobID] = job
					delete(queueing, job.JobID)
				}
			}
		}
	}
	return
}
