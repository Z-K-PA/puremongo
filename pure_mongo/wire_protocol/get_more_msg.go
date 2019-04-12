package wire_protocol

import "pure_mongos/pure_mongo/binary"

//得到更多查询信息的消息
type GetMoreMsg struct {
	Header MsgHeader
	Zero int32
	FullCollName string
	NumToReturn int32
	CursorId int64
}

func NewGetMoreMsg() *GetMoreMsg {
	gMsg := &GetMoreMsg{
		Header:MsgHeader{
			OpCode: OpGetMore,
		},
	}
	return gMsg
}

//序列化
func (gMsg *GetMoreMsg) Marshal(buf *[]byte) (count int32, err error) {
	count += gMsg.Header.Write(buf, count)
	count += binary.WriteInt32(gMsg.Zero, buf, count)
	count += binary.WriteCString(gMsg.FullCollName, buf, count)
	count += binary.WriteInt32(gMsg.NumToReturn, buf, count)
	count += binary.WriteInt64(gMsg.CursorId, buf, count)
	return
}