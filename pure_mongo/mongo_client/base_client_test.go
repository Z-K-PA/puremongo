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

func testPrepare(t *testing.T) (*BaseMongoClient, context.Context, context.CancelFunc, error) {
	mongo_driver_bson.InitDriver()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	cli, err := DialMongoClient(ctx, &net.Dialer{
		KeepAlive: 3 * time.Minute,
	}, "localhost:27017")
	if err != nil {
		t.Errorf("connect error :%+v", err)
	}

	return cli, ctx, cancel, err
}

func TestBaseMongoClient_Master1(t *testing.T) {
	var hashList []map[string]interface{}
	var item map[string]interface{}

	cli, ctx, cancel, err := testPrepare(t)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

	err = cli.QueryWithHandler(ctx, qMsg, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

func TestBaseMongoClient_Master2(t *testing.T) {
	var hashList []map[string]interface{}
	var item map[string]interface{}

	cli, ctx, cancel, err := testPrepare(t)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	wire_protocol.InitIsMasterBuffer()
	err = cli.QueryBufWithHandler(ctx, wire_protocol.IsMasterMsgBuf, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

type XField struct {
	ID int    `bson:"_id"`
	C  string `bosn:"ct"`
}

func TestBaseMongoClient_Insert1(t *testing.T) {
	var rspBody map[string]interface{}
	var rspList []map[string]interface{}

	cli, ctx, cancel, err := testPrepare(t)
	defer cancel()

	inMsg := wire_protocol.NewInsertOneMessage("a", "b", false, XField{ID: 1, C: "1"})
	err = cli.EnhanceMsg(ctx, inMsg, &rspBody, &rspList)
	if err != nil {
		t.Errorf("insert error:%+v", err)
	} else {
		t.Logf("body:%+v", rspBody)
		t.Logf("list:%+v", rspList)
	}
}
