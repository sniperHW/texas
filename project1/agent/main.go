package main

import (
	"context"
	"flag"
	"net"
	"sync"
	"time"

	zaplogger "github.com/sniperHW/clustergo/logger/zap"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var logger *zap.Logger

type sche struct {
	sync.Mutex
	scheAddr string
	socket   *netgo.AsynSocket
	onpacket func(interface{})
}

func (s *sche) Send(o interface{}) {
	var socket *netgo.AsynSocket
	s.Lock()
	socket = s.socket
	if socket == nil {
		dialer := &net.Dialer{}
		conn, err := dialer.Dial("tcp", s.scheAddr)
		if err != nil {
			panic(err)
		}
		codecc := proto.NewCodecc()
		s.socket = netgo.NewAsynSocket(netgo.NewTcpSocket(conn.(*net.TCPConn), codecc), netgo.AsynSocketOption{
			Codec:    codecc,
			AutoRecv: true,
		}).SetCloseCallback(func(_ *netgo.AsynSocket, _ error) {
			s.Lock()
			defer s.Unlock()
			s.socket = nil
		}).SetPacketHandler(func(_ context.Context, _ *netgo.AsynSocket, packet interface{}) error {
			s.onpacket(packet)
			return nil
		}).Recv()
		socket = s.socket
	}
	s.Unlock()
	socket.Send(o)
}

func main() {
	workerID := flag.String("workerID", "worker", "workerID")
	scheAddr := flag.String("scheAddr", "localhost:18889", "scheAddr")
	flag.Parse()

	logger = zaplogger.NewZapLogger(*workerID+".log", "./", "debug", 1024*1024*100, 14, 14, true)

	var jobID atomic.String

	s := &sche{
		scheAddr: *scheAddr,
	}

	s.onpacket = func(p interface{}) {
		switch packet := p.(type) {
		case *proto.DispatchJob:
			logger.Sugar().Debugf("recv job:%s", packet.JobID)
			jobID.Store(packet.JobID)
			go func() {
				time.Sleep(time.Second)
				logger.Sugar().Debugf("job:%s finish,send commit", packet.JobID)
				s.Send(&proto.CommitJobResult{
					JobID:  packet.JobID,
					Result: []byte("hello world"),
				})
			}()
		case *proto.CancelJob:
			jobID.Store("")
		case *proto.AcceptJobResult:
			jobID.Store("")
		}
	}

	for {
		s.Send(&proto.WorkerHeartBeat{
			WorkerID: *workerID,
			Memory:   uint32(1024 * 1024 * 1024),
			JobID:    jobID.Load(),
		})

		time.Sleep(time.Second)
	}
}
