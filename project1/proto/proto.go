package proto

import (
	"encoding/json"
	"errors"
	"net"

	"github.com/sniperHW/clustergo/codec"
	"github.com/sniperHW/clustergo/codec/buffer"
)

const (
	CmdWorkerHeartBeat = uint16(1)
	CmdDispatchJob     = uint16(2)
	CmdCommitJobResult = uint16(3)
	CmdAcceptJobResult = uint16(4)
	CmdCancelJob       = uint16(5)
	CmdSyncPauseFlag   = uint16(6)
	CmdReboot          = uint16(7)
	CmdShutDown        = uint16(8)
)

type TaskReport struct {
	TaskID           string
	ContinuedSeconds int
	IterationNum     int
	Exploit          float64
}

type WorkerHeartBeat struct {
	WorkerID    string
	Memory      uint32
	ThreadCount uint32
	Tasks       []TaskReport
}

type DispatchJob struct {
	TaskID   string
	Cfg      string
	Compress int
}

type CommitJobResult struct {
	TaskID string
	Result string
}

type AcceptJobResult struct {
	TaskID string
}

type CancelJob struct {
	TaskID string
}

type SyncPauseFlag struct {
	Pause int
}

type Reboot struct {
}

type Shutdown struct {
}

func objectToCmd(o interface{}) (uint16, error) {
	switch o.(type) {
	case *WorkerHeartBeat:
		return CmdWorkerHeartBeat, nil
	case *DispatchJob:
		return CmdDispatchJob, nil
	case *CommitJobResult:
		return CmdCommitJobResult, nil
	case *AcceptJobResult:
		return CmdAcceptJobResult, nil
	case *CancelJob:
		return CmdCancelJob, nil
	case *SyncPauseFlag:
		return CmdSyncPauseFlag, nil
	case *Reboot:
		return CmdReboot, nil
	case *Shutdown:
		return CmdShutDown, nil
	}
	return 0, errors.New("invaild object")
}

func bytesToObject(cmd uint16, b []byte) (interface{}, error) {
	var o interface{}
	switch cmd {
	case CmdWorkerHeartBeat:
		o = &WorkerHeartBeat{}
	case CmdDispatchJob:
		o = &DispatchJob{}
	case CmdCommitJobResult:
		o = &CommitJobResult{}
	case CmdAcceptJobResult:
		o = &AcceptJobResult{}
	case CmdCancelJob:
		o = &CancelJob{}
	case CmdSyncPauseFlag:
		o = &SyncPauseFlag{}
	case CmdReboot:
		o = &Reboot{}
	case CmdShutDown:
		o = &Shutdown{}
	}
	if o != nil {
		err := json.Unmarshal(b, o)
		return o, err
	} else {
		return nil, errors.New("invaild object")
	}
}

type Codecc struct {
	codec.LengthPayloadPacketReceiver
	reader buffer.BufferReader
}

func NewCodecc() *Codecc {
	return &Codecc{
		LengthPayloadPacketReceiver: codec.LengthPayloadPacketReceiver{
			Buff:          make([]byte, 1024*1024),
			MaxPacketSize: 1024 * 1024 * 100,
		},
	}
}

func (cc *Codecc) Encode(buffs net.Buffers, o interface{}) (net.Buffers, int) {
	var jsonbytes []byte
	var cmd uint16
	var err error

	if cmd, err = objectToCmd(o); err != nil {
		return buffs, 0
	}

	if jsonbytes, err = json.Marshal(o); err != nil {
		return buffs, 0
	}

	payloadLen := 2 + len(jsonbytes)

	b := make([]byte, 0, 6)
	b = buffer.AppendInt(b, payloadLen)
	b = buffer.AppendUint16(b, cmd)

	return append(buffs, b, jsonbytes), payloadLen + 4
}

func (cc *Codecc) Decode(payload []byte) (interface{}, error) {
	cc.reader.Reset(payload)
	return bytesToObject(cc.reader.GetUint16(), cc.reader.GetAll())
}
