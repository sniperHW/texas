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
	"sort"
	"strings"
	"sync/atomic"

	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"
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
				case *proto.JobFailed:
					s.onJobFailed(as, p)
				}
			}
			return nil
		}).Recv(time.Now().Add(time.Second))
	}); err == nil {
		go serve()
	}

	go s.start()

	logger.Sugar().Debugf("+++++++++++++++++++++++++")

	logger.Sugar().Debugf("scheduler listen on :%s", cfg.WorkerService)

	var btn1 *walk.PushButton
	var btn2 *walk.PushButton
	var btn3 *walk.PushButton
	var btn4 *walk.PushButton

	var mw *walk.MainWindow
	var lb *walk.ListBox
	var label *walk.Label
	var items []listEntry
	model := &listModel{items: items}
	styler := &Styler{
		lb:                  &lb,
		model:               model,
		dpi2StampSize:       make(map[int]walk.Size),
		widthDPI2WsPerLine:  make(map[widthDPI]int),
		textWidthDPI2Height: make(map[textWidthDPI]int),
	}

	if err := (MainWindow{
		AssignTo: &mw,
		Title:    "scheduler",
		MinSize:  Size{200, 200},
		Size:     Size{800, 600},
		Font:     Font{Family: "Segoe UI", PointSize: 9},
		Layout:   VBox{},
		Children: []Widget{
			Label{
				Text:     "总任务数:0,未分配数:0,正在执行:0,已完成:0",
				AssignTo: &label,
			},
			PushButton{
				Text:     "暂停分发任务",
				AssignTo: &btn1,
				OnClicked: func() {
					if atomic.LoadInt32(&s.dispatchFlag) == 1 {
						s.StopDispatch()
						btn1.SetText("恢复分发任务")
					} else {
						s.StartDispatch()
						btn1.SetText("暂停分发任务")
					}
				},
			},
			PushButton{
				Text:     "暂停客户端",
				AssignTo: &btn2,
				OnClicked: func() {
					if atomic.LoadInt32(&s.pauseFlag) == 0 {
						atomic.StoreInt32(&s.pauseFlag, 1)
						btn2.SetText("恢复客户端")
					} else {
						atomic.StoreInt32(&s.pauseFlag, 0)
						btn2.SetText("暂停客户端")
						s.processQueue <- func() {
							s.tryDispatchJob()
						}
					}
				},
			},
			PushButton{
				Text:     "重启所有客户端机器",
				AssignTo: &btn3,
				OnClicked: func() {
					for _, v := range s.workers {
						v.socket.Send(&proto.Reboot{})
					}
				},
			},
			PushButton{
				Text:     "关闭所有客户端机器",
				AssignTo: &btn4,
				OnClicked: func() {
					for _, v := range s.workers {
						v.socket.Send(&proto.Shutdown{})
					}
				},
			},

			Composite{
				DoubleBuffering: true,
				Layout:          VBox{},
				Children: []Widget{
					ListBox{
						AssignTo:       &lb,
						MultiSelection: true,
						Model:          model,
						ItemStyler:     styler,
					},
				},
			},
		},
	}).Create(); err != nil {
		logger.Sugar().Fatal(err)
		//log.Fatal(err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	cancel := make(chan bool, 1)

	/*var nextBroadcast time.Time

	if cfg.PauseBroadcastTime > 0 {
		nextBroadcast = time.Now().Add(time.Duration(cfg.PauseBroadcastTime) * time.Second)
	}*/

	//var pauseTime time.Time

	//if cfg.PauseInterval > 0 {
	//	pauseTime = time.Now().Add(time.Duration(cfg.PauseInterval) * time.Second)
	//}

	//var resumeTime time.Time

	//var pause int32

	go func() {
		for {
			select {
			case <-ticker.C:
				//now := time.Now()

				/*if !pauseTime.IsZero() && now.After(pauseTime) {
					//logger.Sugar().Debugf("pause")
					pauseTime = time.Time{}
					resumeTime = now.Add(time.Duration(cfg.PauseTime) * time.Second)
					atomic.StoreInt32(&s.pauseFlag, 1)
					btn2.Synchronize(func() {
						btn2.SetText("恢复客户端")
					})
				}

				if !resumeTime.IsZero() && now.After(resumeTime) {
					resumeTime = time.Time{}
					pauseTime = now.Add(time.Duration(cfg.PauseInterval) * time.Second)
					atomic.StoreInt32(&s.pauseFlag, 0)
					btn2.Synchronize(func() {
						btn2.SetText("暂停客户端")
						s.processQueue <- func() {
							s.tryDispatchJob()
						}
					})
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

				pause = flag*/

				label.Synchronize(func() {
					unalloc, doing, finish, total := s.getTaskCount()
					label.SetText(fmt.Sprintf("总任务数:%d,未分配数:%d,正在执行:%d,已完成:%d", total, unalloc, doing, finish))
				})
				mw.Synchronize(func() {
					newitems := s.getWorkers()
					//if len(newitems) != len(model.items) {
					//	logger.Sugar().Debugf("%d %d",len(newitems),len(model.items))
					//}

					sort.Slice(newitems, func(i, j int) bool {
						if len(newitems[i].tasks) > len(newitems[j].tasks) {
							return true
						} else if len(newitems[i].tasks) == len(newitems[j].tasks) {
							return newitems[i].worker < newitems[j].worker
						} else {
							return false
						}
					})

					now := time.Now().Unix()

					model.items = model.items[:0]
					for _, v := range newitems {
						fields := []string{fmt.Sprintf("memory:%dG,core:%d", v.memory, v.core)}
						sort.Slice(v.tasks, func(i, j int) bool {
							return v.tasks[i].Id < v.tasks[j].Id
						})
						for _, vv := range v.tasks {
							var flag string
							if vv.exploit > 0 && now-vv.lastChange >= 600 {
								flag = "<--->"
							}
							fields = append(fields, fmt.Sprintf("(%stask:%s,ContinuedSeconds:%ds,IterationNum:%d,Exploit:%0.2f)", flag, vv.Id, vv.continuedSeconds/1000, vv.iterationNum, vv.exploit))
						}
						v.message = strings.Join(fields, " ")
						model.items = append(model.items, v)
					}
					model.PublishItemsReset()
				})

			case <-cancel:
				return
			}
		}
	}()

	mw.Show()
	mw.Run()

	cancel <- true

	/*mw := &MyMainWindow{model: NewEnvModel()}

	if _, err := (MainWindow{
		AssignTo: &mw.MainWindow,
		Title:    "scheduler.exe",
		MinSize:  Size{600, 480},
		//Size:     Size{300, 400},
		Layout:   VBox{MarginsZero: true},
		Children: []Widget{
			PushButton{
				Text: "pause",
				AssignTo: &mw.btn,
				OnClicked: func() {
					if mw.btn.Text() == "pause" {
						mw.btn.SetText("resume")
					} else {
						mw.btn.SetText("pause")
					}
				},
			},
			ListBox{
				AssignTo: &mw.lb,
				Model:    mw.model,
				//OnCurrentIndexChanged: mw.lb_CurrentIndexChanged,
				//OnItemActivated:       mw.lb_ItemActivated,
			},
			/*HSplitter{
				Children: []Widget{
					ListBox{
						AssignTo: &mw.lb,
						Model:    mw.model,
						//OnCurrentIndexChanged: mw.lb_CurrentIndexChanged,
						//OnItemActivated:       mw.lb_ItemActivated,
					},
					//TextEdit{
					//	AssignTo: &mw.te,
					//	ReadOnly: true,
					//},
				},
			},* /
		},
	}.Run()); err != nil {
		logger.Sugar().Error(err)
	}*/
}
