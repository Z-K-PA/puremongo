package wire_protocol

import (
	"errors"
	driver_bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

var (
	ErrFindCursorInvalid = errors.New("查询数据后服务器返回的cursor数据不正确")
)

//查询的参数
type FindOption struct {
	CollectionName string      `bson:"find"`
	Db          string `bson:"$db"`
	Filter         interface{} `bson:"filter"`
	SortVal        interface{} `bson:"sort"`
	Projection     interface{} `bson:"projection"`

	SkipVal     int    `bson:"skip"`
	LimitVal    int    `bson:"limit"`
	SingleBatch bool   `bson:"singleBatch"`
	MaxTimeMSVal int `bson:"maxTimeMS"`
}

//序列化
func (option *FindOption) MarshalBsonWithBuffer(buf *[]byte, pos int32) (docLen int32, err error) {
	begin := pos
	pos += binary.WriteInt32(0, buf, pos)

	newBuf := (*buf)[pos:pos]

	_, newBuf, err = bsonx.Elem{
		Key:"find",
		Value:bsonx.String(option.CollectionName),
	}.Value.MarshalAppendBSONValue(newBuf)
	if err != nil {
		return
	}
	pos += int32(len(newBuf))
	newBuf = newBuf[pos:pos]

	_, newBuf, err = bsonx.Elem{
		Key:"$db",
		Value:bsonx.String(option.Db),
	}.Value.MarshalAppendBSONValue(newBuf)
	if err != nil {
		return
	}
	pos += int32(len(newBuf))
	newBuf = newBuf[pos:pos]

	_, newBuf, err = bsonx.Elem{
		Key:"filter",
		Value:bsonx.String(option.Db),
	}.Value.MarshalAppendBSONValue(newBuf)
	if err != nil {
		return
	}
	pos += int32(len(newBuf))
	newBuf = newBuf[pos:pos]

	newBuf = append(newBuf, )
}

//分批查询的参数
type GetMore struct {
	CursorId       int64  `bson:"getMore"`
	CollectionName string `bson:"collection"`
}

//分批查询的参数-- 带服务器超时
type GetMoreWithTimeout struct {
	CursorId       int64  `bson:"getMore"`
	CollectionName string `bson:"collection"`
	MaxTimeMS      int    `bson:"maxTimeMS"`
}

//查询的参数 -- 带服务器超时
type FindMetaWithTimeout struct {
	FindOption   `bson:",inline"`

}

//注销cursor的参数
type CursorKill struct {
	CollectionName string  `bson:"killCursors"`
	CursorList     []int64 `bson:"cursors"`
	Db             string  `bson:"$db"`
}

//Find返回结果
type FindResult struct {
	//指令本身相关的返回信息
	APIMsgRspCode `bson:",inline"`
	//cursorId
	CursorId int64
	//返回结果
	DocList bson.BsonDocList
}

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

func parseString(val driver_bson.RawValue) string {
	strVal, ok := val.StringValueOK()
	if ok {
		return strVal
	} else {
		return ""
	}
}

func (fRes *FindResult) parseCursor(cursor driver_bson.RawValue, batchKey string) (err error) {
	var elements []driver_bson.RawElement
	cursorDoc, ok := cursor.DocumentOK()
	if !ok {
		err = ErrFindCursorInvalid
		return
	}
	elements, err = cursorDoc.Elements()
	for _, element := range elements {
		switch element.Key() {
		case "id":
			fRes.CursorId = int64(parseNumber2Int(element.Value()))
		case batchKey:
			_, err = fRes.DocList.ParseFromBuf(element.Value().Value)
		}
	}
	return
}

func (fRes *FindResult) FromBuffer(buf []byte, batchKey string) (err error) {
	var elements []driver_bson.RawElement

	bsonRaw := driver_bson.Raw(buf)
	elements, err = bsonRaw.Elements()
	if err != nil {
		return
	}

	for _, element := range elements {
		switch element.Key() {
		case "ok":
			fRes.OK = parseNumber2Int(element.Value())
		case "errmsg":
			fRes.ErrMsg = parseString(element.Value())
		case "codeName":
			fRes.CodeName = parseString(element.Value())
		case "code":
			fRes.Code = parseNumber2Int(element.Value())
		case "batchKey":
			err = fRes.parseCursor(element.Value(), batchKey)
		}
	}
	return
}
