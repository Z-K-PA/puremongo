package bson

//序列化bson-带buffer
type MarshalBsonWithBufferFunc func(in interface{}, buf *[]byte, pos int32) (bsonLen int32, err error)

//序列化bson
type MarshalBsonFunc func(in interface{}) (out []byte, err error)

//反序列化bson
type UnMarshalBsonFunc func(in []byte, out interface{}) (err error)

//Wire-protocol中文档采用 key-value形式
type DocPair struct {
	Name  string
	Value interface{}
}

//文档接口
type IDoc interface {
	MarshalBuffer(buf *[]byte, pos int32) (docLen int32, err error)
	AddDoc(docPairs ...DocPair)
}

//为文档添加内容
type AddDocumentsFunc func(docPairs ...DocPair) IDoc

type DriverMode int

const (
	//没有定义
	DriverModeUndefine = DriverMode(0)
	//使用的mgo相关模式
	DriverModeMgo = DriverMode(1)
	//使用mongodb golang driver官方推荐的相关模式
	DriverModeMongoDriver = DriverMode(2)
)

func (d DriverMode) String() string {
	switch d {
	case DriverModeUndefine:
		return "没有定义驱动的模式"
	case DriverModeMgo:
		return "采用mgo的模式"
	case DriverModeMongoDriver:
		return "采用mongodb golang driver官方推荐模式"
	default:
		return "被设置成了错误的模式"
	}
}

/*--------------------包的全局变量,主要是函数的定义和模式的定义-----*/

//序列化bson-带buffer
var MarshalBsonWithBuffer MarshalBsonWithBufferFunc

//序列化bson
var MarshalBson MarshalBsonFunc

//反序列化bson
var UnMarshalBson UnMarshalBsonFunc

//为文档添加内容
var AddDoc AddDocumentsFunc

//Driver中采用的模式
var CurrentDriverMode DriverMode
