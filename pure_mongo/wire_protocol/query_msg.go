package wire_protocol

import (
	"pure_mongos/pure_mongo/bson"
)

const (
	QueryFlagTailCursor      = 1 << 1
	QueryFlagSlaveOk         = 1 << 2
	QueryFlagOpLogReplay     = 1 << 3
	QueryFlagNoCursorTimeout = 1 << 4
	QueryFlagAwaitData       = 1 << 5
	QueryFlagExhaust         = 1 << 6
	QueryFlagPartial         = 1 << 7
)

type QueryMsg struct {
	Header       MsgHeader
	Flags        int32
	FullCollName string
	NumToSkip    int32
	NumToReturn  int32
	Doc          bson.IDoc
}

func NewQueryMsg() *QueryMsg {
	qMsg := &QueryMsg{
		Header: MsgHeader{
			OpCode: OpQuery,
		},
	}
	return qMsg
}

//设置查询flag
func (qMsg *QueryMsg) SetFlag(flags ...int32) {
	//清空后再做
	qMsg.Flags = 0
	for _, flag := range flags {
		qMsg.Flags |= flag
	}
}

//添加查询doc
func (qMsg *QueryMsg) AddDoc(pairs ...bson.DocPair) {
	if qMsg.Doc == nil {
		qMsg.Doc = bson.AddDoc(pairs...)
	} else {
		qMsg.Doc.AddDoc(pairs...)
	}
}

//序列化
func (qMsg *QueryMsg) Marshal(buf *[]byte) (count int32, err error) {
	var docCount int32

	pos := int32(0)

	pos += qMsg.Header.Write(buf, pos)

	pos += WriteInt32(qMsg.Flags, buf, pos)

	pos += WriteCString(qMsg.FullCollName, buf, pos)

	pos += WriteInt32(qMsg.NumToSkip, buf, pos)

	pos += WriteInt32(qMsg.NumToReturn, buf, pos)

	docCount, err = qMsg.Doc.MarshalBuffer(buf, pos)
	count = pos + docCount
	return
}
