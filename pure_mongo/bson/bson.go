package bson

type MarshalBsonWithBufferFunc func(in interface{}, buf []byte) (out []byte, err error)
type MarshalBsonFunc func(in interface{}) (out []byte, err error)
type UnMarshalBsonFunc func(in []byte, out interface{}) (err error)

type DocPair struct {
	Name  string
	Value interface{}
}
type IDoc interface {
	MarshalBuffer(buf []byte) (out []byte, err error)
}

type AddDocumentsFunc func(docPairs ...DocPair) IDoc

var MarshalBsonWithBuffer MarshalBsonWithBufferFunc
var MarshalBson MarshalBsonFunc
var UnMarshalBson UnMarshalBsonFunc
var AddDoc AddDocumentsFunc

