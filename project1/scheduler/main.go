package main

import (
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
	toml := flag.String("toml", "toml.toml", "toml")
	flag.Parse()

	cfg, err := LoadConfig(*toml)

	if err != nil {
		panic(err)
	}

	logger = zaplogger.NewZapLogger("scheduler.log", cfg.Log.LogDir, cfg.Log.LogLevel, 1024*1024*100, 14, 14, cfg.Log.EnableStdout)

	s := &sche{
		doing:        map[string]*job{},
		workers:      map[string]*worker{},
		taskGroups:   map[string]*taskGroup{},
		processQueue: make(chan func()),
		cfg:          cfg,
	}

	if err = s.init(); err != nil {
		panic(err)
	}

	if _, serve, err := netgo.ListenTCP("tcp", cfg.WorkerService, func(conn *net.TCPConn) {
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

	/*if _, serve, err := netgo.ListenTCP("tcp", *tcpAddr, func(conn *net.TCPConn) {
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
	}*/

	s.start()
}
