package wire_protocol

import (
	"errors"
	"pure_mongos/pure_mongo/binary"
)

var (
	ErrInvalidReturnDoc = errors.New("奇葩了，服务器返回的文档格式有问题")
)

//返回的文档内容
type ReturnDocList []byte

//读取一个回复的doc
func readReturnDoc(d ReturnDocList, pos int32) (doc []byte, docSize int32, err error) {
	lBuf := int32(len(d))
	if pos == lBuf {
		return
	}
	if pos+4 > lBuf {
		err = ErrInvalidReturnDoc
		return
	}
	docSize = binary.ReadInt32(d, pos)
	if pos+docSize > lBuf {
		err = ErrInvalidReturnDoc
		return
	}
	if d[pos+docSize-1] != 0 {
		err = ErrInvalidReturnDoc
		return
	}
	doc = d[pos : pos+docSize]
	return
}

//读取回复消息中所以的文档到字节流切片
func (d ReturnDocList) Parse() (bufList [][]byte, err error) {
	var doc []byte
	var docSize int32

	pos := int32(0)
	for {
		doc, docSize, err = readReturnDoc(d, pos)
		if err != nil {
			return
		}
		if doc == nil {
			return
		}
		bufList = append(bufList, doc)
		pos += docSize
	}
}

//回复消息
type ReplyMsg struct {
	Header         MsgHeader
	ResponseFlags  int32
	CursorId       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      ReturnDocList
}
