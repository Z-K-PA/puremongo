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
	//API msg头长度
	//1.如果是body（kind-1 byte，bsonDoc-head-4）
	//2.如果是seq doc(kind-1 byte,length-4 byte)
	APIMsgHeadSize = HeaderLen + 4 + 1 + 4
)

//API Msg消息体中的一种格式块
type BodyBlock struct {
	Kind   byte
	Doc    bson.BsonDoc
	Val    interface{}
	setted bool
}

//序列化
func (b *BodyBlock) MarshalBsonWithBuffer(buf *[]byte, pos int32) (count int32, err error) {
	if !b.setted {
		return
	}
	pos += binary.WriteByte(b.Kind, buf, pos)
	switch b.Val.(type) {
	case bson.IDoc:
		iDoc := b.Val.(bson.IDoc)
		count, err = iDoc.MarshalBsonWithBuffer(buf, pos)
	default:
		count, err = bson.MarshalBsonWithBuffer(b.Val, buf, pos)
	}

	count += 1
	return
}

//API Msg消息体中的另外一种格式块
type SeqDocListBlock struct {
	Kind     byte
	Length   int32
	Identify string
	DocList  bson.BsonDocList
	ValList  []interface{}
	setted   bool
}

//序列化
func (sb *SeqDocListBlock) MarshalBsonWithBuffer(buf *[]byte, pos int32) (count int32, err error) {
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

//API消息
type APIMsg struct {
	Header     MsgHeader
	Flags      int32
	Body       BodyBlock
	SeqDocList SeqDocListBlock
}

func NewAPIMsg() *APIMsg {
	enMsg := &APIMsg{
		Header: MsgHeader{
			OpCode: OpMsg,
		},
	}
	return enMsg
}

//设置消息体
func (apiMsg *APIMsg) SetBodyDoc(val interface{}) {
	apiMsg.Body.Kind = DocBodyType
	apiMsg.Body.Val = val
	apiMsg.Body.setted = true
}

//设置消息体中的seq identify
func (apiMsg *APIMsg) SetSeqDoc(identify string) {
	if apiMsg.SeqDocList.ValList == nil {
		apiMsg.SeqDocList.ValList = make([]interface{}, 0, 8)
	}
	apiMsg.SeqDocList.Kind = SeqDocListType
	apiMsg.SeqDocList.Identify = identify
	apiMsg.SeqDocList.setted = true
}

//添加消息体中的seq doc
func (apiMsg *APIMsg) AddSeqDoc(val interface{}) {
	apiMsg.SeqDocList.ValList = append(apiMsg.SeqDocList.ValList, val)
}

//序列化
func (apiMsg *APIMsg) MarshalBsonWithBuffer(buf *[]byte) (count int32, err error) {
	pos := int32(0)
	bobyDocSize := int32(0)
	seqDocSize := int32(0)

	pos += apiMsg.Header.Write(buf, pos)
	pos += binary.WriteInt32(apiMsg.Flags, buf, pos)

	bobyDocSize, err = apiMsg.Body.MarshalBsonWithBuffer(buf, pos)
	if err != nil {
		return
	}
	pos += bobyDocSize

	seqDocSize, err = apiMsg.SeqDocList.MarshalBsonWithBuffer(buf, pos)
	if err != nil {
		return
	}
	pos += seqDocSize
	count = pos

	return
}

//从字节流中分拣出body和seq doc
func (apiMsg *APIMsg) formBuffer(buf []byte) (count int32, err error) {
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
		apiMsg.Body.Kind = DocBodyType
		count, err = apiMsg.Body.Doc.ParseFromBuf(buf[pos:])
		if err != nil {
			return
		}
		count += pos
		apiMsg.Body.setted = true
		return
	} else if kind == SeqDocListType {
		//seq doc list
		apiMsg.SeqDocList.Kind = SeqDocListType
		apiMsg.SeqDocList.Length, err = binary.ReadInt32(buf, pos)
		if err != nil {
			return
		}

		if len(buf[pos:]) < int(apiMsg.SeqDocList.Length) {
			err = ErrInvalidMsgFromSrv
			return
		}

		pos += 4
		apiMsg.SeqDocList.Identify, count, err = binary.ReadCString(buf, pos)
		if err != nil {
			return
		}
		pos += count

		count, err = apiMsg.SeqDocList.DocList.ParseFromBuf(buf[pos:])
		if err != nil {
			return
		}
		count += pos
		apiMsg.SeqDocList.setted = true
		return
	} else {
		err = ErrInvalidMsgFromSrv
		return
	}
}

//从网络数据中生成
func (apiMsg *APIMsg) FromBuffer(header MsgHeader, buf []byte) (err error) {
	var count int32

	if len(buf) < APIMsgHeadSize {
		err = ErrInvalidMsgFromSrv
		return
	}

	apiMsg.Header = header
	pos := int32(HeaderLen)

	apiMsg.Flags, err = binary.ReadInt32(buf, pos)
	if err != nil {
		return
	}
	pos += 4

	for {
		count, err = apiMsg.formBuffer(buf[pos:])
		if count == 0 && err == nil {
			return
		}
		if err != nil {
			return
		}
		pos += count
	}
}

//API Msg消息的返回值
type APIMsgRspCode struct {
	//服务器是否执行操作
	//0-消息格式有问题，服务器不执行
	//1-服务器执行了操作，但不代表插入成功，结果需要结合其它字段检查
	OK int `bson:"ok"`
	//插入消息出错的错误码
	Code int `bson:"code"`
	//插入消息出错的错误码名称
	CodeName string `bson:"codeName"`
	//插入消息出错的错误原因
	ErrMsg string `bson:"errmsg"`
}
