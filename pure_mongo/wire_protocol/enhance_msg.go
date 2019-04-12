package wire_protocol

import (
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

const (
	DocBodyType    = 0
	SeqDocListType = 1
)

//EnhanceMsg消息体中的一种格式块
type BodyBlock struct {
	Kind   byte
	Doc    bson.BsonDoc
	Val    interface{}
	setted bool
}

//序列化
func (b *BodyBlock) Marshal(buf *[]byte, pos int32) (count int32, err error) {
	if !b.setted {
		return
	}
	pos += binary.WriteByte(b.Kind, buf, pos)
	count, err = bson.MarshalBsonWithBuffer(b.Val, buf, pos)
	count += 1
	return
}

//EnhanceMsg消息体中的另外一种格式块
type SeqDocListBlock struct {
	Kind     byte
	Length   int32
	Identify string
	DocList  bson.BsonDocList
	ValList  []interface{}
	setted   bool
}

//序列化
func (sb *SeqDocListBlock) Marshal(buf *[]byte, pos int32) (count int32, err error) {
	if !sb.setted {
		return
	}
	//回填锚点
	anchorPos := pos

	pos += binary.WriteByte(sb.Kind, buf, pos)
	pos += binary.WriteInt32(0, buf, pos)
	pos += binary.WriteCString(sb.Identify, buf, pos)

	count = pos - anchorPos
	_count := int32(0)

	for _, val := range sb.ValList {
		_count, err = bson.MarshalBsonWithBuffer(val, buf, pos)
		pos += _count
		count += _count
	}
	//重写length
	binary.WriteInt32(count, buf, anchorPos+1)
	return
}

//消息
type EnhanceMsg struct {
	Header     MsgHeader
	Flags      int32
	Body       BodyBlock
	SeqDocList SeqDocListBlock
}

func NewEnhanceMsg() *EnhanceMsg {
	enMsg := &EnhanceMsg{
		Header: MsgHeader{
			OpCode: OpMsg,
		},
	}
	return enMsg
}

//设置消息体
func (enMsg *EnhanceMsg) SetBodyDoc(val interface{}) {
	enMsg.Body.Kind = DocBodyType
	enMsg.Body.Val = val
	enMsg.Body.setted = true
}

//设置消息体中的seq identify
func (enMsg *EnhanceMsg) SetSeqDoc(identify string) {
	if enMsg.SeqDocList.ValList == nil {
		enMsg.SeqDocList.ValList = make([]interface{}, 0, 8)
	}
	enMsg.SeqDocList.Kind = SeqDocListType
	enMsg.SeqDocList.Identify = identify
	enMsg.SeqDocList.setted = true
}

//添加消息体中的seq doc
func (enMsg *EnhanceMsg) AddSeqDoc(val interface{}) {
	enMsg.SeqDocList.ValList = append(enMsg.SeqDocList.ValList, val)
}

//序列化
func (enMsg *EnhanceMsg) Marshal(buf *[]byte) (count int32, err error) {
	pos := int32(0)
	bobyDocSize := int32(0)
	seqDocSize := int32(0)

	pos += enMsg.Header.Write(buf, pos)
	pos += binary.WriteInt32(enMsg.Flags, buf, pos)

	bobyDocSize, err = enMsg.Body.Marshal(buf, pos)
	if err != nil {
		return
	}
	pos += bobyDocSize

	seqDocSize, err = enMsg.SeqDocList.Marshal(buf, pos)
	if err != nil {
		return
	}
	pos += seqDocSize
	count = pos

	return
}