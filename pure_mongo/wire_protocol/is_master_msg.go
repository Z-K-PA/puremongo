package wire_protocol

import (
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/bson"
)

//固定格式的消息
var (
	IsMasterMsgBuf []byte
)

func InitIsMasterBuffer() {
	if IsMasterMsgBuf == nil {
		IsMasterMsgBuf = initIsMasterMsgBuffer()
	}
}

func initIsMasterMsgBuffer() []byte {
	var queryMetaBuf []byte

	queryMetaMsg := NewQueryMsg()
	//设置为系统表
	queryMetaMsg.FullCollName = "admin.$cmd"
	//什么查询flag都不用设置
	queryMetaMsg.Flags = 0
	//返回值填-1
	queryMetaMsg.NumToReturn = -1
	//添加doc
	var isMasterVal interface{}
	switch bson.CurrentDriverMode {
	case bson.DriverModeMgo:
		isMasterVal = 1
	case bson.DriverModeMongoDriver:
		isMasterVal = bsonx.Int32(1)
	default:
		panic("没有调用初始化相关函数,请在mgo_bson包中或mongo_driver_bson包中调用InitDriver")
	}
	queryMetaMsg.AddDoc(
		bson.DocPair{
			Name:  "isMaster",
			Value: isMasterVal,
		},
	)

	count, err := queryMetaMsg.MarshalBsonWithBuffer(&queryMetaBuf)
	if err != nil {
		panic(err)
	}

	return queryMetaBuf[:count]
}
