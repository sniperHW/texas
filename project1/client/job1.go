package main

import (
	"flag"
	"net"
	"time"

	"github.com/sniperHW/netgo"
	"github.com/sniperHW/texas/project1/proto"
)

func main() {
	scheAddr := flag.String("scheAddr", "localhost:18899", "scheAddr")
	jobId := flag.String("jobId", "job1", "jobId")
	flag.Parse()

	dialer := &net.Dialer{}
	conn, err := dialer.Dial("tcp", *scheAddr)
	if err != nil {
		panic(err)
	}
	codecc := proto.NewCodecc()

	tcpConn := netgo.NewTcpSocket(conn.(*net.TCPConn), codecc)
	//SendBuffers(buffs net.Buffers, deadline ...time.Time) (int64, error)
	var buffs net.Buffers
	buffs, _ = codecc.Encode(buffs, &proto.NewJob{
		JobID:      *jobId,
		MemoryNeed: 1024 * 1024,
		CfgPath:    "./",
		ResultPath: "./",
	})

	tcpConn.(interface {
		SendBuffers(net.Buffers, ...time.Time) (int64, error)
	}).SendBuffers(buffs)

}
