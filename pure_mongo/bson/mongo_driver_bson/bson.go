package mongo_driver_bson

import (
	driver_bson "go.mongodb.org/mongo-driver/bson"
	driver_bsonx "go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/bson"
)

func InitDriver() {
	bson.MarshalBsonWithBuffer = marshalBsonWithBuffer
	bson.MarshalBson = driver_bson.Marshal
	bson.UnMarshalBson = driver_bson.Unmarshal
	bson.AddDoc = addDoc
	bson.CurrentDriverMode = bson.DriverModeMongoDriver
}

func marshalBsonWithBuffer(in interface{}, buf *[]byte, pos int32) (bsonLen int32, err error) {
	var out []byte

	out, err = driver_bson.MarshalAppend((*buf)[pos:pos], in)
	if err != nil {
		return
	}
	bsonLen = binary.AppendBytesIfNeed(buf, out, pos)
	return
}

type DriverDoc struct {
	driver_bsonx.Doc
}

func (d *DriverDoc) MarshalBsonWithBuffer(buf *[]byte, pos int32) (bsonLen int32, err error) {
	var out []byte

	out, err = d.Doc.AppendMarshalBSON((*buf)[pos:pos])
	if err != nil {
		return
	}
	bsonLen = binary.AppendBytesIfNeed(buf, out, pos)
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
