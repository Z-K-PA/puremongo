package wire_protocol

import (
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

const (
	//回复消息头固定大小
	ReplyMsgHeadSize = HeaderLen + 4 + 8 + 4 + 4
)

//回复消息
type ReplyMsg struct {
	Header         MsgHeader
	ResponseFlags  int32
	CursorId       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      bson.BsonDocList
}

//从网络数据中生成
func (rMsg *ReplyMsg) FromBuffer(header MsgHeader, buf []byte) (err error) {

	if len(buf) < ReplyMsgHeadSize {
		err = ErrInvalidMsgFromSrv
		return
	}

	rMsg.Header = header
	pos := int32(HeaderLen)

	rMsg.ResponseFlags, err = binary.ReadInt32(buf, pos)
	if err != nil {
		return
	}
	pos += 4
	rMsg.CursorId, err = binary.ReadInt64(buf, pos)
	if err != nil {
		return
	}
	pos += 8
	rMsg.StartingFrom, err = binary.ReadInt32(buf, pos)
	if err != nil {
		return
	}
	pos += 4
	rMsg.NumberReturned, err = binary.ReadInt32(buf, pos)
	if err != nil {
		return
	}
	pos += 4

	_, err = rMsg.Documents.ParseFromBuf(buf[ReplyMsgHeadSize:])
	return
}
