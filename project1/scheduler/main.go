package main

////go build -ldflags="-H windowsgui"

import (
	"context"
	"flag"
	"net"
	"runtime"

	//"os"
	//"os/signal"
	//"syscall"
	"time"

	"github.com/boltdb/bolt"
	zaplogger "github.com/sniperHW/clustergo/logger/zap"
	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
	"go.uber.org/zap"

	"fmt"
	"strings"
	"sync/atomic"
)

var logger *zap.Logger

func FormatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

func main() {

	toml := flag.String("toml", "toml.toml", "toml")
	flag.Parse()

	cfg, err := LoadConfig(*toml)

	if err != nil {
		panic(err)
	}

	logger = zaplogger.NewZapLogger("scheduler.log", cfg.Log.LogDir, cfg.Log.LogLevel, 1024*1024*100, 14, 14, cfg.Log.EnableStdout)

	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			logger.Sugar().Panicln(FormatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
		}
	}()

	s := &sche{
		doing:        map[string]*task{},
		tasks:        map[string]*task{},
		workers:      map[string]*worker{},
		taskGroups:   map[string]*taskGroup{},
		processQueue: make(chan func()),
		die:          make(chan struct{}),
		stopc:        make(chan struct{}),
		cfg:          cfg,
		dispatchFlag: 1,
	}

	s.db, err = bolt.Open(cfg.DB, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer s.db.Close()

	if err = s.init(); err != nil {
		panic(err)
	}

	if _, serve, err := netgo.ListenTCP("tcp", cfg.WorkerService, func(conn *net.TCPConn) {
		//logger.Debug("on new worker")
		codecc := proto.NewCodecc()
		netgo.NewAsynSocket(netgo.NewTcpSocket(conn, codecc), netgo.AsynSocketOption{
			Codec:           codecc,
			AutoRecv:        true,
			AutoRecvTimeout: time.Second * 15,
		}).SetPacketHandler(func(_ context.Context, as *netgo.AsynSocket, packet interface{}) error {
			s.processQueue <- func() {
				//logger.Sugar().Debugf("on packet %v", packet)
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

	go s.start()

	logger.Sugar().Debugf("scheduler listen on :%s", cfg.WorkerService)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	cancel := make(chan bool, 1)

	var nextBroadcast time.Time

	if cfg.PauseBroadcastTime > 0 {
		nextBroadcast = time.Now().Add(time.Duration(cfg.PauseBroadcastTime) * time.Second)
	}

	var pauseTime time.Time

	if cfg.PauseInterval > 0 {
		pauseTime = time.Now().Add(time.Duration(cfg.PauseInterval) * time.Second)
	}

	var resumeTime time.Time

	var pause int32

	go func() {
		for {
			select {
			case <-ticker.C:
				now := time.Now()

				if !pauseTime.IsZero() && now.After(pauseTime) {
					//logger.Sugar().Debugf("pause")
					pauseTime = time.Time{}
					resumeTime = now.Add(time.Duration(cfg.PauseTime) * time.Second)
					atomic.StoreInt32(&s.pauseFlag, 1)
				}

				if !resumeTime.IsZero() && now.After(resumeTime) {
					resumeTime = time.Time{}
					pauseTime = now.Add(time.Duration(cfg.PauseInterval) * time.Second)
					atomic.StoreInt32(&s.pauseFlag, 0)
				}

				flag := atomic.LoadInt32(&s.pauseFlag)

				if !nextBroadcast.IsZero() && (flag != pause || now.After(nextBroadcast)) {
					nextBroadcast = time.Now().Add(time.Duration(cfg.PauseBroadcastTime) * time.Second)
					s.processQueue <- func() {

						packet := &proto.SyncPauseFlag{
							Pause: int(atomic.LoadInt32(&s.pauseFlag)),
						}
						logger.Sugar().Debugf("broadcast flag %d", packet.Pause)
						for _, v := range s.workers {
							v.socket.Send(packet)
						}
					}
				}
				pause = flag
			case <-cancel:
				return
			}
		}
	}()

	ch := make(chan struct{})

	<-ch

	cancel <- true

}
