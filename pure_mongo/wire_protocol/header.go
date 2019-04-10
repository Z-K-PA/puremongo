package wire_protocol

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
	h.MsgLen = ReadInt32(buf, 0)
	h.ReqId = ReadInt32(buf, 4)
	h.ResTo = ReadInt32(buf, 8)
	h.OpCode = ReadInt32(buf, 12)
}

//写入消息头
func (h *MsgHeader) Write(buf []byte) {
	WriteInt32(h.MsgLen, buf, 0)
	WriteInt32(h.ReqId, buf, 4)
	WriteInt32(h.ResTo, buf, 8)
	WriteInt32(h.OpCode, buf, 12)
}
