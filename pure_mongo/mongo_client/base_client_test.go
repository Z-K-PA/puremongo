package connection

import (
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"pure_mongos/pure_mongo/wire_protocol"
	"testing"
)

func TestMongoClient_SendQueryRecvReply(t *testing.T) {
	mongo_driver_bson.InitDriver()
	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

}
