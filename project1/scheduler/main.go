package main

import (
	"container/list"
	"context"
	"flag"
	"net"
	"time"

	zaplogger "github.com/sniperHW/clustergo/logger/zap"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
	"go.uber.org/zap"
)

var logger *zap.Logger

func main() {

	logname := flag.String("logname", "scheduler.log", "logname")
	storage := flag.String("storage", "./storage.txt", "storage")
	workerTcpAddr := flag.String("workerTcpAddr", ":18889", "workerTcpAddr")
	tcpAddr := flag.String("tcpAddr", ":18899", "tcpAddr")
	flag.Parse()

	logger = zaplogger.NewZapLogger(*logname, "./", "debug", 1024*1024*100, 14, 14, true)

	var err error

	s := &sche{
		jobs:         map[string]*Job{},
		jobQueue:     list.New(),
		workers:      map[string]*worker{},
		processQueue: make(chan func()),
	}

	s.storage, err = NewFileStorage(*storage)

	if err != nil {
		panic(err)
	}

	finish, doing, queueing, err := s.storage.Load()

	if err != nil {
		panic(err)
	}

	s.jobFinish = finish
	s.jobDoing = doing

	for _, v := range s.jobFinish {
		s.jobs[v.JobID] = v
	}
	for _, v := range s.jobDoing {
		v.deadline = time.Now().Add(time.Second * 30)
		s.jobs[v.JobID] = v
	}
	for _, v := range queueing {
		s.jobQueue.PushBack(v)
		s.jobs[v.JobID] = v
	}

	if _, serve, err := netgo.ListenTCP("tcp", *workerTcpAddr, func(conn *net.TCPConn) {
		logger.Debug("on new worker")
		codecc := proto.NewCodecc()
		netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codecc), netgo.AsynSocketOption{
			Codec:           codecc,
			AutoRecv:        true,
			AutoRecvTimeout: time.Second * 15,
		}).SetPacketHandler(func(_ context.Context, as *netgo.AsynSocket, packet interface{}) error {
			s.processQueue <- func() {
				logger.Sugar().Debugf("on packet %v", packet)
				switch p := packet.(type) {
				case *proto.WorkerHeartBeat:
					s.onWorkerHeartBeat(as, p)
				case *proto.CommitJobResult:
					s.onCommitJobResult(as, p)
				}
			}
			return nil
		}).Recv(time.Now().Add(time.Second))
	}); err == nil {
		go serve()
	}

	if _, serve, err := netgo.ListenTCP("tcp", *tcpAddr, func(conn *net.TCPConn) {
		logger.Debug("on new client")
		codecc := proto.NewCodecc()
		netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codecc), netgo.AsynSocketOption{
			Codec: codecc,
		}).SetPacketHandler(func(_ context.Context, as *netgo.AsynSocket, packet interface{}) error {
			s.processQueue <- func() {
				switch p := packet.(type) {
				case *proto.NewJob:
					s.onNewJob(&Job{
						JobID:      p.JobID,
						MemNeed:    p.MemoryNeed,
						CfgPath:    p.CfgPath,
						ResultPath: p.ResultPath,
					})
				}
			}
			return nil
		}).Recv()
	}); err == nil {
		go serve()
	}

	s.start()
}
