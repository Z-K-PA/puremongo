package bson

type MarshalBsonWithBufferFunc func(in interface{}, buf []byte) (out []byte, err error)
type MarshalBsonFunc func(in interface{}) (out []byte, err error)
type UnMarshalBsonFunc func(in []byte, out interface{}) (err error)

var MarshalBsonWithBuffer MarshalBsonWithBufferFunc
var MarshalBson MarshalBsonFunc
var UnMarshalBson UnMarshalBsonFunc

