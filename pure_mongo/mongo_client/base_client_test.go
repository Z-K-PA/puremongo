package connection

import (
	"context"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"net"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"pure_mongos/pure_mongo/wire_protocol"
	"testing"
	"time"
)

func TestBaseMongoClient_SendQueryRecvReply1(t *testing.T) {
	mongo_driver_bson.InitDriver()
	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cli, err := DialMongoClient(ctx, &net.Dialer{
		KeepAlive: 3 * time.Minute,
	}, "localhost:27017")
	if err != nil {
		t.Errorf("connect error :%+v", err)
	}

	var hashList []map[string]interface{}

	var item map[string]interface{}
	var handler = func() {
		hashList = append(hashList, item)
	}

	err = cli.QueryWithHandler(ctx, qMsg, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	}else {
		t.Logf("hash list is %+v", hashList)
	}
}

func TestBaseMongoClient_SendQueryRecvReply2(t *testing.T) {
	mongo_driver_bson.InitDriver()
	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cli, err := DialMongoClient(ctx, &net.Dialer{
		KeepAlive: 3 * time.Minute,
	}, "localhost:27017")
	if err != nil {
		t.Errorf("connect error :%+v", err)
	}

	var hashList []map[string]interface{}

	var item map[string]interface{}
	var handler = func() {
		hashList = append(hashList, item)
	}

	wire_protocol.InitIsMasterBuffer()
	err = cli.QueryBufWithHandler(ctx, wire_protocol.IsMasterMsgBuf, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	}else {
		t.Logf("hash list is %+v", hashList)
	}
}