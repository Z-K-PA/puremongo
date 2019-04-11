package wire_protocol

import (
	"bytes"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mgo_bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"testing"
)

func TestInitQueryMetaBufferWithMgo(t *testing.T) {
	mgo_bson.InitDriver()
	InitQueryMetaBuffer()
	t.Logf("buf count:%+v\n", len(QueryMetaBuffer))
}

func TestInitQueryMetaBufferWithMongoDriver(t *testing.T) {
	mongo_driver_bson.InitDriver()
	InitQueryMetaBuffer()
	t.Logf("buf count:%+v\n", len(QueryMetaBuffer))
}

func TestInitQueryCompare(t *testing.T) {
	mgo_bson.InitDriver()
	t.Logf("driver mode :%+v\n", bson.CurrentDriverMode)
	buf1 := initQueryMetaBuffer()
	mongo_driver_bson.InitDriver()
	t.Logf("driver mode :%+v\n", bson.CurrentDriverMode)
	buf2 := initQueryMetaBuffer()

	t.Logf("buf1 count:%+v, buf2 count:%+v\n", len(buf1), len(buf2))
	if !bytes.Equal(buf1, buf2) {
		t.Fail()
	}
}
