package bson

import (
	"errors"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/limit"
	"reflect"
)

var (
	ErrInvalidBsonDoc = errors.New("bson文档内容出错")
	ErrLargeBsonDoc   = errors.New("bson文档内容过大")
)

//bson文档
type BsonDoc struct {
	Buf []byte
	Val interface{}
}

//从字节流中分拣出bson
func (bc *BsonDoc) ParseFromBuf(buf []byte) (bSize int32, err error) {
	lBuf := int32(len(buf))
	if lBuf == 0 {
		return
	}
	bSize, err = binary.ReadInt32(buf, 0)
	if err != nil {
		return
	}
	if bSize > limit.MaxBsonDocSize {
		err = ErrLargeBsonDoc
		return
	}
	if lBuf < bSize {
		err = ErrInvalidBsonDoc
		return
	}
	if buf[bSize-1] != 0 {
		err = ErrInvalidBsonDoc
		return
	}
	bc.Buf = buf[:bSize]
	return
}

//反序列化
func (bc *BsonDoc) Unmarshal(val interface{}) (err error) {
	return UnMarshalBson(bc.Buf, val)
}

//bson文档列表
type BsonDocList []BsonDoc

//从字节流中分拣出bson到列表中
func (bL *BsonDocList) ParseFromBuf(buf []byte) (bSize int32, err error) {
	var bsonDoc BsonDoc

	pos := int32(0)
	docSize := int32(0)

	lBuf := len(buf)
	if lBuf > limit.MaxBsonDocSize {
		err = ErrLargeBsonDoc
		return
	}

	for {
		docSize, err = bsonDoc.ParseFromBuf(buf[pos:])
		if docSize == 0 && err == nil {
			//没有数据了
			return
		}
		if err != nil {
			return
		}
		if *bL == nil {
			*bL = make([]BsonDoc, 0, 8)
		}
		*bL = append(*bL, bsonDoc)
		pos += docSize
		bSize += docSize
	}

	return
}

//反序列化
func (bl BsonDocList) Unmarshal(val interface{}) (err error) {
	docLen := len(bl)
	if docLen == 0 {
		return
	}

	resultv := reflect.ValueOf(val)
	slicev := resultv.Elem()

	if slicev.Kind() == reflect.Interface {
		slicev = slicev.Elem()
	}
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()

	i := 0

	for i = 0; i < docLen; i++ {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			err = UnMarshalBson((bl)[i].Buf, elemp.Interface())
			if err != nil {
				return
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			err = UnMarshalBson((bl)[i].Buf, slicev.Index(i).Addr().Interface())
			if err != nil {
				return
			}
		}
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return
}

//反序列化处理函数
type UnmarshalDocListHandler func()

//反序列化 -- 带处理函数
func (bl BsonDocList) UnmarshalWithHandler(handler UnmarshalDocListHandler, val interface{}) (err error) {
	for _, doc := range bl {
		err = doc.Unmarshal(val)
		if err != nil {
			return
		}
		handler()
	}
	return
}

//处理bson binary函数
type HandleBsonDocFunc func(rawBuf []byte) (err error)
