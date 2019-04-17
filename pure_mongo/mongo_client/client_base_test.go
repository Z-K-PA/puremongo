package connection

import (
	"context"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"net"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mgo_bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"pure_mongos/pure_mongo/wire_protocol"
	"reflect"
	"testing"
	"time"
)

func testPrepare1(t *testing.T, duration time.Duration) (*MongoClient, context.Context, context.CancelFunc, error) {
	mongo_driver_bson.InitDriver()

	ctx, cancel := context.WithTimeout(context.Background(), duration)

	cli, err := DialMongoClient(ctx, &net.Dialer{
		KeepAlive: 3 * time.Minute,
	}, "localhost:27017")
	if err != nil {
		t.Errorf("connect error :%+v", err)
	}

	return cli, ctx, cancel, err
}

func testPrepare2(t *testing.T, duration time.Duration) (*MongoClient, context.Context, context.CancelFunc, error) {
	mgo_bson.InitDriver()

	ctx, cancel := context.WithTimeout(context.Background(), duration)

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

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

	err = cli.queryWithHandler(ctx, qMsg, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

func TestBaseMongoClient_Master2(t *testing.T) {
	var hashList []map[string]interface{}
	var item map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	wire_protocol.InitIsMasterBuffer()
	err = cli.queryBufWithHandler(ctx, wire_protocol.IsMasterMsgBuf, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

type XField struct {
	ID int    `bson:"_id"`
	C  string `bson:"ct"`
}

func TestBaseMongoClient_Insert1(t *testing.T) {
	var rspBody map[string]interface{}
	var rspList []map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	inMsg := wire_protocol.NewInsertOneMessage("a", "b", false, XField{ID: 1, C: "1"})
	err = cli.runAPIMsg(ctx, inMsg, &rspBody, &rspList)
	if err != nil {
		t.Errorf("insert error:%+v", err)
	} else {
		t.Logf("body:%+v", rspBody)
		t.Logf("list:%+v", rspList)
	}
}

func TestBaseMongoClient_Insert2(t *testing.T) {
	var rspBody map[string]interface{}
	var rspList []map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	x := make([]XField, 2)
	x[0] = XField{ID: 2, C: "1"}
	x[1] = XField{ID: 3, C: "2"}

	itemsv := reflect.ValueOf(x)
	itemsLen := itemsv.Len()

	for i := 0; i < itemsLen; i++ {
		t.Logf("item:%+v", itemsv.Index(i))
		t.Logf("item:%+v", x[i])
	}

	inMsg, err := wire_protocol.NewInsertManyMessage("a", "b", false, x)
	if err != nil {
		t.Errorf("new insert many msg error:%+v", err)
	}

	err = cli.runAPIMsg(ctx, inMsg, &rspBody, &rspList)
	if err != nil {
		t.Errorf("insert error:%+v", err)
	} else {
		t.Logf("body:%+v", rspBody)
		t.Logf("list:%+v", rspList)
	}
}

func TestBaseMongoClient_Insert3(t *testing.T) {
	var rspBody map[string]interface{}
	var rspList []map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	x := make([]interface{}, 2)
	x[0] = XField{ID: 9, C: "1"}
	x[1] = XField{ID: 6, C: "2"}

	inMsg := wire_protocol.NewInsertManyMessageI("a", "b", false, x)
	err = cli.runAPIMsg(ctx, inMsg, &rspBody, &rspList)
	if err != nil {
		t.Errorf("insert error:%+v", err)
	} else {
		t.Logf("body:%+v", rspBody)
		t.Logf("list:%+v", rspList)
	}
}

func TestBaseMongoClient_Insert4(t *testing.T) {
	var rspBody map[string]interface{}
	var rspList []map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	x := make([]interface{}, 2)
	x[0] = XField{ID: 9, C: "1"}
	x[1] = XField{ID: 6, C: "2"}

	inMsg := wire_protocol.NewInsertManyMessageI("a", "b", true, x)
	err = cli.runAPIMsg(ctx, inMsg, &rspBody, &rspList)
	if err != nil {
		t.Errorf("insert error:%+v", err)
	} else {
		t.Logf("body:%+v", rspBody)
		t.Logf("list:%+v", rspList)
	}
}
