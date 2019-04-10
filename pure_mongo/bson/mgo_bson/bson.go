package mgo_bson

import (
	mgo_bson "github.com/globalsign/mgo/bson"
	"pure_mongos/pure_mongo/bson"
)

func init() {
	bson.MarshalBsonWithBuffer = mgo_bson.MarshalBuffer
	bson.MarshalBson = mgo_bson.Marshal
	bson.UnMarshalBson = mgo_bson.Unmarshal
	bson.AddDoc = addDoc
}

type MgoDoc struct {
	mgo_bson.D
}

func (d *MgoDoc) MarshalBuffer(buf []byte) (out []byte, err error) {
	out, err = mgo_bson.MarshalBuffer(d.D, buf)
	return
}

func addDoc(docPairs ...bson.DocPair) bson.IDoc {
	doc := make(mgo_bson.D, 0, 16)
	for _, docPair := range docPairs {
		doc = append(doc, mgo_bson.DocElem{
			Name:  docPair.Name,
			Value: docPair.Value,
		})
	}
	return &MgoDoc{
		D: doc,
	}
}
