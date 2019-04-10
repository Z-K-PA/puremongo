package mongo_driver_bson

import (
	driver_bson "go.mongodb.org/mongo-driver/bson"
	driver_bsonx "go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/bson"
)

func init() {
	bson.MarshalBsonWithBuffer = marshalAppend
	bson.MarshalBson = driver_bson.Marshal
	bson.UnMarshalBson = driver_bson.Unmarshal
	bson.AddDoc = addDoc
}

func marshalAppend(in interface{}, buf []byte) (out []byte, err error) {
	return driver_bson.MarshalAppend(buf, in)
}

type DriverDoc struct {
	driver_bsonx.Doc
}

func (d *DriverDoc) MarshalBuffer(buf []byte) (out []byte, err error) {
	out, err = d.Doc.MarshalBSON()
	if err != nil {
		return
	}
	if len(out) <= len(buf) {
		//可以拷贝
		copy(buf, out)
	}
	return
}

func addDoc(docPairs ...bson.DocPair) bson.IDoc {
	doc := make([]driver_bsonx.Elem, 0, 16)
	for _, docPair := range docPairs {
		doc = append(doc, driver_bsonx.Elem{
			Key:   docPair.Name,
			Value: docPair.Value.(driver_bsonx.Val),
		})
	}
	return &DriverDoc{
		Doc: doc,
	}
}
