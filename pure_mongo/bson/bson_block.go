package bson

import (
	"errors"
	"pure_mongos/pure_mongo/binary"
)

const (
	MaxBsonDocSize = 40 * 1024 * 1024 //bson文档最大设定为40M
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
	if bSize > MaxBsonDocSize {
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

//bson文档列表
type BsonDocList []BsonDoc

//从字节流中分拣出bson到列表中
func (bL *BsonDocList) ParseFromBuf(buf []byte) (bSize int32, err error) {
	var bsonDoc BsonDoc

	pos := int32(0)
	docSize := int32(0)

	lBuf := len(buf)
	if lBuf > MaxBsonDocSize {
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
