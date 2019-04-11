package mgo_bson

import (
	mgo_bson "github.com/globalsign/mgo/bson"
	"pure_mongos/pure_mongo/bson"
)

func InitDriver() {
	bson.MarshalBsonWithBuffer = mgo_bson.MarshalBuffer
	bson.MarshalBson = mgo_bson.Marshal
	bson.UnMarshalBson = mgo_bson.Unmarshal
	bson.AddDoc = addDoc
	bson.CurrentDriverMode = bson.DriverModeMgo
}

type MgoDoc struct {
	mgo_bson.D
}

func (d *MgoDoc) MarshalBuffer(buf *[]byte, pos int32) (docLen int32, err error) {
	var out []byte

	out, err = mgo_bson.MarshalBuffer(d.D, (*buf)[pos:pos])
	docLen = int32(len(out))

	length := int(pos + docLen)
	if cap(*buf) < length {
		//内存不够，需要重新分配
		*buf = append(*buf, out...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}

	return
}

func (d *MgoDoc) AddDoc(docPairs ...bson.DocPair) {
	for _, docPair := range docPairs {
		d.D = append(d.D, mgo_bson.DocElem{
			Name:  docPair.Name,
			Value: docPair.Value,
		})
	}
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
