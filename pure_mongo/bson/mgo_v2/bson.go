package mgo_bson

import (
	mgo_v2_bson "github.com/globalsign/mgo/bson"
	"pure_mongos/pure_mongo/bson"
)

func init() {
	bson.MarshalBsonWithBuffer = mgo_v2_bson.MarshalBuffer
	bson.MarshalBson = mgo_v2_bson.Marshal
	bson.UnMarshalBson = mgo_v2_bson.Unmarshal
}
