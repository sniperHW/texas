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
)

type Task struct {
	TaskID     string
	CfgPath    string
	ResultPath string
}

type WorkerHeartBeat struct {
	WorkerID string
	Memory   uint32
	JobID    string
}

type DispatchJob struct {
	Tasks []Task
}

type CommitJobResult struct {
	JobID string
}

type AcceptJobResult struct {
	JobID string
}

type CancelJob struct {
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
			MaxPacketSize: 1024 * 1024,
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
