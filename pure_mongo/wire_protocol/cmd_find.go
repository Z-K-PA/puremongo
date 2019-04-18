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
	Db             string      `bson:"$db"`
	Filter         interface{} `bson:"filter"`
	SortVal        interface{} `bson:"sort"`
	Projection     interface{} `bson:"projection"`

	SkipVal      int32 `bson:"skip"`
	LimitVal     int32 `bson:"limit"`
	MaxTimeMSVal int32 `bson:"maxTimeMS"`
	SingleBatch  bool  `bson:"singleBatch"`
}

//序列化
func (option *FindOption) MarshalBsonWithBuffer(buf *[]byte, pos int32) (docLen int32, err error) {

	//起始位置留4字节保存大小
	begin := pos
	pos += 4

	bsonLen := int32(0)

	//collection信息
	bsonLen, err = marshalItem(buf, pos, "find", bsonx.String(option.CollectionName))
	if err != nil {
		return
	}
	pos += bsonLen

	//db信息
	bsonLen, err = marshalItem(buf, pos, "$db", bsonx.String(option.Db))
	if err != nil {
		return
	}
	pos += bsonLen

	//过滤条件
	bsonLen, err = marshalDocItem(buf, pos, "filter", option.Filter)
	if err != nil {
		return
	}
	pos += bsonLen

	//排序条件
	if option.SortVal != nil {
		bsonLen, err = marshalDocItem(buf, pos, "sort", option.SortVal)
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//返回的字段选定条件
	if option.Projection != nil {
		bsonLen, err = marshalDocItem(buf, pos, "projection", option.Projection)
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//cursor跳过的条目数
	if option.SkipVal > 0 {
		bsonLen, err = marshalItem(buf, pos, "skip", bsonx.Int32(option.SkipVal))
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//cursor选定的条目数
	if option.LimitVal > 0 {
		bsonLen, err = marshalItem(buf, pos, "limit", bsonx.Int32(option.LimitVal))
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//服务器查询最多使用的毫秒数，如果超时，服务器会终止查询
	//如果为0，则没有限制
	if option.MaxTimeMSVal > 0 {
		bsonLen, err = marshalItem(buf, pos, "maxTimeMS", bsonx.Int32(option.MaxTimeMSVal))
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//是否在第一次cursor返回后就干掉cursor（服务器干掉，不需要客户端再发请求）
	if option.SingleBatch {
		bsonLen, err = marshalItem(buf, pos, "singleBatch", bsonx.Boolean(option.SingleBatch))
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//补零
	pos += binary.WriteByte(0, buf, pos)

	//回填大小
	docLen = pos - begin
	binary.WriteInt32(docLen, buf, begin)

	return
}

func (option *FindOption) _MarshalBsonWithBuffer(buf *[]byte, pos int32) (docLen int32, err error) {
	if option.SortVal == nil {
		option.SortVal = map[string]interface{}{}
	}
	if option.Projection == nil {
		option.Projection = map[string]interface{}{}
	}

	return bson.MarshalBsonWithBuffer(*option, buf, pos)
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
	MaxTimeMS      int32  `bson:"maxTimeMS"`
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
	CursorId int64 `bson:"id"`
	//返回结果
	DocList bson.ArrayDoc
}

//解析游标
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

//解析结果
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
		case "cursor":
			err = fRes.parseCursor(element.Value(), batchKey)
		}
	}
	return
}