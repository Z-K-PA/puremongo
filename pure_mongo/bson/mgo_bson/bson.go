package mgo_bson

import (
	mgo_bson "github.com/globalsign/mgo/bson"
	"pure_mongos/pure_mongo/bson"
)

func init() {
	bson.MarshalBsonWithBuffer = mgo_bson.MarshalBuffer
	bson.MarshalBson = mgo_bson.Marshal
	bson.UnMarshalBson = mgo_bson.Unmarshal
}
