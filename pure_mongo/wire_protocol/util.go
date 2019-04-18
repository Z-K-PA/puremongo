package wire_protocol

import (
	driver_bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

//序列化单项
func marshalItem(buf *[]byte, pos int32, key string, val bsonx.Val) (itemLen int32, err error) {
	var bsonBuf []byte

	beginPos := pos
	vType := val.Type()
	pos += binary.WriteByte(byte(vType), buf, pos)
	pos += binary.WriteCString(key, buf, pos)

	cursorBuf := (*buf)[pos:pos]
	_, bsonBuf, err = bsonx.Elem{
		Key:   key,
		Value: val,
	}.Value.MarshalAppendBSONValue(cursorBuf)
	if err != nil {
		return
	}
	pos += binary.AppendBytesIfNeed(buf, bsonBuf, pos)

	itemLen = pos - beginPos

	return
}

//序列化文档单项
func marshalDocItem(buf *[]byte, pos int32, key string, val interface{}) (docLen int32, err error) {
	beginPos := pos
	pos += binary.WriteByte(byte(bsontype.EmbeddedDocument), buf, pos)
	pos += binary.WriteCString(key, buf, pos)
	docLen, err = bson.MarshalBsonWithBuffer(val, buf, pos)
	pos += docLen

	docLen = pos - beginPos
	return
}

//解析元素值到int
func parseNumber2Int(val driver_bson.RawValue) int {
	switch val.Type {
	case driver_bson.TypeInt32:
		return int(val.Int32())
	case driver_bson.TypeInt64:
		return int(val.Int64())
	case driver_bson.TypeDouble:
		return int(val.Double())
	}
	return 0
}

//解析元素值到string
func parseString(val driver_bson.RawValue) string {
	strVal, ok := val.StringValueOK()
	if ok {
		return strVal
	} else {
		return ""
	}
}

//解析API Msg
func ParseAPIMsg(header MsgHeader, buf []byte) (eMsg *APIMsg, err error) {
	if header.OpCode != OpMsg {
		err = ErrInvalidMsgFromSrv
		return
	}

	eMsg = &APIMsg{}
	err = eMsg.FromBuffer(header, buf)
	return
}
