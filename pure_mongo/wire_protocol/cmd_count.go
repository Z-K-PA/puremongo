package wire_protocol

import (
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/binary"
)

//Count参数
type CountOption struct {
	CollectionName string      `bson:"count"`
	Db             string      `bson:"$db"`
	Query          interface{} `bson:"query"`
	Limit          int32       `bson:"limit"`
	Skip           int32       `bson:"skip"`
}

//序列化
func (option *CountOption) MarshalBsonWithBuffer(buf *[]byte, pos int32) (docLen int32, err error) {
	//起始位置留4字节保存大小
	begin := pos
	pos += 4

	bsonLen := int32(0)

	//collection信息
	bsonLen, err = marshalItem(buf, pos, "count", bsonx.String(option.CollectionName))
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
	bsonLen, err = marshalDocItem(buf, pos, "query", option.Query)
	if err != nil {
		return
	}
	pos += bsonLen

	//cursor跳过的条目数
	if option.Skip > 0 {
		bsonLen, err = marshalItem(buf, pos, "skip", bsonx.Int32(option.Skip))
		if err != nil {
			return
		}
		pos += bsonLen
	}

	//cursor选定的条目数
	if option.Limit > 0 {
		bsonLen, err = marshalItem(buf, pos, "limit", bsonx.Int32(option.Limit))
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

type CountN struct {
	//总数
	N int `bson:"n"`
}

type CountResult struct {
	//指令本身相关的返回信息
	APIMsgRspCode `bson:",inline"`
	//总数
	N int `bson:"n"`
}
