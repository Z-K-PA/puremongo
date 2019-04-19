package connection

import (
	"context"
	"fmt"
	"net"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
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

func TestMongoClient_InsertMany1(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	l := 200
	x := make([]XField, l)
	for i := 0; i < l; i++ {
		x[i].ID = i
		x[i].C = fmt.Sprintf("%d", i)
	}

	rsp, err := cli.InsertMany(ctx, "a", "b", x, false)
	if err != nil {
		t.Errorf("写入数据失败 %+v", err)
	} else {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Logf("has error:%+v", rsp.WriteErrors.HasErrors())
		t.Logf("all is dup error:%+v", rsp.WriteErrors.AllIsDuplicateErr())

		for _, wl := range rsp.WriteErrors {
			t.Logf("write error:%+v", wl)
		}
	}
}

func TestMongoClient_InsertMany2(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	l := 30
	x := make([]XField, l)
	xs := make([]byte, 1024*1024)

	for i := range xs {
		xs[i] = 'a'
	}

	for i := 0; i < l; i++ {
		x[i].ID = -i
		x[i].C = string(xs)
	}

	rsp, err := cli.InsertMany(ctx, "a", "b", x, false)
	if err != nil {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Errorf("写入数据失败 %+v", err)
	} else {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Logf("has error:%+v", rsp.WriteErrors.HasErrors())
		t.Logf("all is dup error:%+v", rsp.WriteErrors.AllIsDuplicateErr())

		for _, wl := range rsp.WriteErrors {
			t.Logf("write error:%+v", wl)
		}
	}
}

func TestMongoClient_InsertMany3(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	l := 30
	x := make([]interface{}, l)
	xs := make([]byte, 1024*1024)

	for i := range xs {
		xs[i] = 'a'
	}

	for i := 0; i < l; i++ {
		x[i] = XField{
			ID: i,
			C:  string(xs),
		}
	}

	rsp, err := cli.InsertManyI(ctx, "a", "b", x, false)
	if err != nil {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Errorf("写入数据失败 %+v", err)
	} else {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Logf("has error:%+v", rsp.WriteErrors.HasErrors())
		t.Logf("all is dup error:%+v", rsp.WriteErrors.AllIsDuplicateErr())

		for _, wl := range rsp.WriteErrors {
			t.Logf("write error:%+v", wl)
		}
	}
}

func TestMongoClient_InsertMany4(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	xs := make([]byte, 1024*1024*10)

	l := 3

	for i := range xs {
		xs[i] = 'a'
	}
	x := make([]interface{}, l)
	for i := 0; i < l; i++ {
		x[i] = XField{
			ID: i,
			C:  string(xs),
		}
	}

	rsp, err := cli.InsertManyI(ctx, "a", "b", x, false)
	if err != nil {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Errorf("写入数据失败 %+v", err)
	} else {
		t.Logf("rsp head :%+v", rsp.APIMsgRspCode)
		t.Logf("rsp num :%+v", rsp.Number)
		t.Logf("has error:%+v", rsp.WriteErrors.HasErrors())
		t.Logf("all is dup error:%+v", rsp.WriteErrors.AllIsDuplicateErr())

		for _, wl := range rsp.WriteErrors {
			t.Logf("write error:%+v", wl)
		}
	}
}
