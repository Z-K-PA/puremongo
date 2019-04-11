package wire_protocol

import "pure_mongos/pure_mongo/binary"

const (
	HeaderLen = 16 //MsgLen
)

//request type
const (
	OpReply        int32 = 1
	_              int32 = 1001
	OpUpdate       int32 = 2001
	OpInsert       int32 = 2002
	_              int32 = 2003
	OpQuery        int32 = 2004
	OpGetMore      int32 = 2005
	OpDelete       int32 = 2006
	OpKillCursors  int32 = 2007
	OpCommand      int32 = 2010
	OpCommandReply int32 = 2011
	OpCompressed   int32 = 2012
	OpMsg          int32 = 2013
)

//消息头定义
type MsgHeader struct {
	MsgLen int32 // total message size, including this
	ReqId  int32 // identifier for this message
	ResTo  int32 //   (used in responses from db)
	OpCode int32 // request type
}

//读取消息头
func (h *MsgHeader) Read(buf []byte) {
	h.MsgLen = binary.ReadInt32(buf, 0)
	h.ReqId = binary.ReadInt32(buf, 4)
	h.ResTo = binary.ReadInt32(buf, 8)
	h.OpCode = binary.ReadInt32(buf, 12)
}

//写入消息头
func (h *MsgHeader) Write(buf *[]byte, pos int32) (count int32) {
	binary.WriteInt32(h.MsgLen, buf, pos)
	binary.WriteInt32(h.ReqId, buf, pos+4)
	binary.WriteInt32(h.ResTo, buf, pos+8)
	binary.WriteInt32(h.OpCode, buf, pos+12)

	return HeaderLen
}
