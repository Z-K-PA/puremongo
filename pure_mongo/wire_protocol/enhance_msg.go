package wire_protocol

import (
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

const (
	//body type
	DocBodyType = 0
	//seq doc list type
	SeqDocListType = 1
	//Enhance msg头长度 1.如果是body（kind-1byte，bsonDoc-head-4）2.如果是seq doc(kind-1byte,length-4byte)
	EnhanceMsgHeadSize = HeaderLen + 4 + 1 + 4
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

	pos += binary.WriteByte(sb.Kind, buf, pos)

	//回填锚点
	anchorPos := pos

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
	binary.WriteInt32(count, buf, anchorPos)
	count++
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
	enMsg.SeqDocList.setted = true
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

//从字节流中分拣出body和seq doc
func (enMsg *EnhanceMsg) formBuffer(buf []byte) (count int32, err error) {
	var kind byte
	var pos int32

	//没有需要解析的字节流
	if len(buf) == 0 {
		return
	}

	kind, err = binary.ReadByte(buf, 0)
	if err != nil {
		return
	}
	pos++

	if kind == DocBodyType {
		//body
		enMsg.Body.Kind = DocBodyType
		count, err = enMsg.Body.Doc.ParseFromBuf(buf[pos:])
		if err != nil {
			return
		}
		count += pos
		enMsg.Body.setted = true
		return
	} else if kind == SeqDocListType {
		//seq doc list
		enMsg.SeqDocList.Kind = SeqDocListType
		enMsg.SeqDocList.Length, err = binary.ReadInt32(buf, pos)
		if err != nil {
			return
		}

		if len(buf[pos:]) < int(enMsg.SeqDocList.Length) {
			err = ErrInvalidMsgFromSrv
			return
		}

		pos += 4
		enMsg.SeqDocList.Identify, count, err = binary.ReadCString(buf, pos)
		if err != nil {
			return
		}
		pos += count

		count, err = enMsg.SeqDocList.DocList.ParseFromBuf(buf[pos:])
		if err != nil {
			return
		}
		count += pos
		enMsg.SeqDocList.setted = true
		return
	} else {
		err = ErrInvalidMsgFromSrv
		return
	}
}

//从网络数据中生成
func (enMsg *EnhanceMsg) FromBuffer(header MsgHeader, buf []byte) (err error) {
	var count int32

	if len(buf) < EnhanceMsgHeadSize {
		err = ErrInvalidMsgFromSrv
		return
	}

	enMsg.Header = header
	pos := int32(HeaderLen)

	enMsg.Flags, err = binary.ReadInt32(buf, pos)
	if err != nil {
		return
	}
	pos += 4

	for {
		count, err = enMsg.formBuffer(buf[pos:])
		if count == 0 && err == nil {
			return
		}
		if err != nil {
			return
		}
		pos += count
	}
}
