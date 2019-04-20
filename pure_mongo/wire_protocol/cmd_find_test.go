package wire_protocol

import (
	"bytes"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"testing"
)

func TestFindOption_MarshalBsonWithBuffer(t *testing.T) {
	mongo_driver_bson.InitDriver()
	option := FindOption{
		CollectionName: "1",
		Db:             "2",
		Filter:         bson.Hash{},
		Sort:           bson.Hash{},
		Projection:     bson.Hash{},

		Skip:        1,
		Limit:       1,
		MaxTimeMS:   1,
		SingleBatch: true,
	}

	buf1 := make([]byte, 512)
	l1, _ := option._MarshalBsonWithBuffer(&buf1, 10)

	buf2 := make([]byte, 512)
	l2, _ := option.MarshalBsonWithBuffer(&buf2, 10)

	t.Logf("%+v", buf1[10:10+l1])
	t.Logf("%+v", buf2[10:10+l2])

	if !bytes.Equal(buf1, buf2) {
		t.Errorf(" not equal")
	}
}
