package legacy

import (
	"bytes"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mgo_bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"testing"
)

func TestInitQueryMetaBufferWithMgo(t *testing.T) {
	mgo_bson.InitDriver()
	InitIsMasterBuffer()
	t.Logf("buf count:%+v\n", len(IsMasterMsgBuf))
}

func TestInitQueryMetaBufferWithMongoDriver(t *testing.T) {
	mongo_driver_bson.InitDriver()
	InitIsMasterBuffer()
	t.Logf("buf count:%+v\n", len(IsMasterMsgBuf))
}

func TestInitQueryCompare(t *testing.T) {
	mgo_bson.InitDriver()
	t.Logf("driver mode :%+v\n", bson.CurrentDriverMode)
	buf1 := initIsMasterMsgBuffer()
	mongo_driver_bson.InitDriver()
	t.Logf("driver mode :%+v\n", bson.CurrentDriverMode)
	buf2 := initIsMasterMsgBuffer()

	t.Logf("buf1 count:%+v, buf2 count:%+v\n", len(buf1), len(buf2))
	t.Logf("buf1 cap:%+v, buf2 cap:%+v\n", cap(buf1), cap(buf2))
	if !bytes.Equal(buf1, buf2) {
		t.Fail()
	}
}