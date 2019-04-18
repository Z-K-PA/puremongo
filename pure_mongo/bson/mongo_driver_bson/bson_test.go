package mongo_driver_bson

import (
	"bytes"
	driver_bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
	"testing"
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

func marshalTest(buf *[]byte, pos int32) (docLen int32, err error) {
	//起始位置留4字节保存大小
	begin := pos
	pos += 4

	bsonLen := int32(0)

	bsonLen, err = marshalItem(buf, pos, "a", bsonx.String("a"))
	if err != nil {
		return
	}
	pos += bsonLen

	//过滤条件
	bsonLen, err = marshalDocItem(buf, pos, "b", map[string]interface{}{"A": "A"})
	if err != nil {
		return
	}
	pos += bsonLen

	pos += binary.WriteByte(0, buf, pos)

	//回填大小
	binary.WriteInt32(pos, buf, begin)
	docLen = pos
	return
}

func TestEncodeS(t *testing.T) {
	InitDriver()

	v := make(map[string]interface{})

	v["a"] = "a"
	v["b"] = map[string]interface{}{"A": "A"}

	buf, err := driver_bson.Marshal(v)

	t.Logf("err of buf:%+v", err)
	t.Logf("buf:%+v", buf)

	xbuf := make([]byte, 60)
	l, err := marshalTest(&xbuf, 0)

	t.Logf("buf %+v", xbuf[:l])
	t.Logf("buf %+v", xbuf)
	t.Logf("len is %+v", l)

}

func TestPanic(t *testing.T) {
	x := make([]byte, 2)
	y := x[2:]

	i := bytes.IndexByte(y, 0)

	t.Logf("%+v", x)
	t.Logf("%+v", y)
	t.Logf("%+v", i)

	i1 := int(-1)
	i2 := int32(i1)
	t.Logf("%+v -- %+v", i1, i2)

}