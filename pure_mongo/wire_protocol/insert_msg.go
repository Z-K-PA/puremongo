package wire_protocol

import (
	"errors"
	"reflect"
)

var (
	ErrInsertManyValNotASlice = errors.New("插入多条记录时传入的参数不是切片")
)

//插入数据的相关信息结构
type InsertMeta struct {
	CollectionName string `bson:"insert"`
	Ordered        bool   `bson:"ordered"`
	Db             string `bson:"$db"`
}

//新建插入单条内容的消息
func NewInsertOneMessage(db string, collection string, ordered bool, item interface{}) *APIMsg {
	enMsg := NewAPIMsg()
	enMsg.SetBodyDoc(InsertMeta{
		Db:             db,
		CollectionName: collection,
		Ordered:        ordered,
	})
	enMsg.SetSeqDoc("documents")
	enMsg.AddSeqDoc(item)
	return enMsg
}

//新建插入多条内容的消息
func NewInsertManyMessage(db string, collection string, ordered bool, items interface{}) (
	enMsg *APIMsg, err error) {

	switch reflect.TypeOf(items).Kind() {
	case reflect.Slice:
	default:
		err = ErrInsertManyValNotASlice
		return
	}

	enMsg = NewAPIMsg()
	enMsg.SetBodyDoc(InsertMeta{
		Db:             db,
		CollectionName: collection,
		Ordered:        ordered,
	})
	enMsg.SetSeqDoc("documents")

	itemsv := reflect.ValueOf(items)
	itemsLen := itemsv.Len()

	for i := 0; i < itemsLen; i++ {
		enMsg.AddSeqDoc(itemsv.Index(i).Interface())
	}
	return
}

//新建插入多条内容的消息
func NewInsertManyMessageI(db string, collection string, ordered bool, items []interface{}) *APIMsg {

	enMsg := NewAPIMsg()
	enMsg.SetBodyDoc(InsertMeta{
		Db:             db,
		CollectionName: collection,
		Ordered:        ordered,
	})
	enMsg.SetSeqDoc("documents")

	for _, item := range items {
		enMsg.AddSeqDoc(item)
	}

	return enMsg
}

//插入数据的错误返回值
type WriteError struct {
	//出错的index
	Index int `bson:"index"`
	//错误码
	Code int `bson:"code"`
	//错误消息
	ErrMsg string `bson:"errmsg"`
}

//写入数据错误列表
type WriteErrList []WriteError

func (wL WriteErrList) HasErrors() bool {
	return len(wL) > 0
}

func (wL WriteErrList) AllIsDuplicateErr() bool {
	for _, w := range wL {
		if w.Code != 11000 {
			return false
		}
	}
	return true
}

type InsertResult struct {
	//指令本身相关的返回信息
	APIMsgRspCode `bson:",inline"`
	//插入消息的条数
	Number int `bson:"n"`
	//插入数据如果不成功，不成功的原因可以在WriteErrorList中查找
	WriteErrors WriteErrList `bson:"writeErrors"`
}
