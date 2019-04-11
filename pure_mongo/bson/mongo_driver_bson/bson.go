package mongo_driver_bson

import (
	driver_bson "go.mongodb.org/mongo-driver/bson"
	driver_bsonx "go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/bson"
)

func InitDriver() {
	bson.MarshalBsonWithBuffer = marshalAppend
	bson.MarshalBson = driver_bson.Marshal
	bson.UnMarshalBson = driver_bson.Unmarshal
	bson.AddDoc = addDoc
	bson.CurrentDriverMode = bson.DriverModeMongoDriver
}

func marshalAppend(in interface{}, buf []byte) (out []byte, err error) {
	return driver_bson.MarshalAppend(buf, in)
}

type DriverDoc struct {
	driver_bsonx.Doc
}

func (d *DriverDoc) MarshalBuffer(buf *[]byte, pos int32) (docLen int32, err error) {
	var out []byte

	out, err = d.Doc.MarshalBSON()
	if err != nil {
		return
	}

	docLen = int32(len(out))
	if len((*buf)[pos:]) < int(docLen) {
		//内存不够，需要重新分配
		*buf = append(*buf, out...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	} else {
		//直接拷贝结果
		copy((*buf)[pos:], out)
	}

	return
}

func (d *DriverDoc) AddDoc(docPairs ...bson.DocPair) {
	for _, docPair := range docPairs {
		d.Doc = append(d.Doc, driver_bsonx.Elem{
			Key:   docPair.Name,
			Value: docPair.Value.(driver_bsonx.Val),
		})
	}
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
