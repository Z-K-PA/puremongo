package wire_protocol

import (
	"bytes"
	driver_bson "go.mongodb.org/mongo-driver/bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"testing"
)

func TestFindOption_MarshalBsonWithBuffer(t *testing.T) {
	mongo_driver_bson.InitDriver()
	option := FindOption{
		CollectionName: "1",
		Db:             "2",
		Filter:         map[string]interface{}{},
		SortVal:        map[string]interface{}{},
		Projection:     map[string]interface{}{},

		SkipVal:      1,
		LimitVal:     1,
		MaxTimeMSVal: 1,
		SingleBatch:  true,
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

func TestNullDocBuffer(t *testing.T) {
	var elements []driver_bson.RawElement

	bsonRaw := driver_bson.Raw(nil)
	elements, err := bsonRaw.Elements()
	if err != nil {
		t.Errorf("error is %+v", err)
	}

	for _, element := range elements {
		t.Logf("element key:%+v", element.Key())
	}
	return
}
