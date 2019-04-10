package mongo_driver_bson

import (
	driver_bson "go.mongodb.org/mongo-driver/bson"
	"pure_mongos/pure_mongo/bson"
)

func init() {
	bson.MarshalBsonWithBuffer = marshalAppend
	bson.MarshalBson = driver_bson.Marshal
	bson.UnMarshalBson = driver_bson.Unmarshal
}

func marshalAppend(in interface{}, buf []byte) (out []byte, err error) {
	return driver_bson.MarshalAppend(buf, in)
}
