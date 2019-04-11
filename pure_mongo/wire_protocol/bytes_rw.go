package wire_protocol

import (
	"bytes"
	"errors"
)

var (
	ErrReadCString = errors.New("解析CString出错")
)

//读取int32
func ReadInt32(buf []byte, pos int32) int32 {
	return (int32(buf[pos])) | (int32(buf[pos+1]) << 8) | (int32(buf[pos+2]) << 16) | (int32(buf[pos+3]) << 24)
}

//读取int64
func ReadInt64(buf []byte, pos int32) int64 {
	return (int64(buf[pos])) | (int64(buf[pos+1]) << 8) | (int64(buf[pos+2]) << 16) | (int64(buf[pos+3]) << 24) |
		(int64(buf[pos+4]) << 32) | (int64(buf[pos+5]) << 40) | (int64(buf[pos+6]) << 48) | (int64(buf[pos+7]) << 56)
}

//读取CString
func ReadCString(buf []byte, pos int32) (val string, cstringLen int32, err error) {
	nullPos := bytes.IndexByte(buf[pos:], 0)
	if nullPos == -1 {
		err = ErrReadCString
		return
	}
	_nullPos := int32(nullPos)
	cstringLen = _nullPos + 1
	val = string(buf[pos : pos+_nullPos])
	return
}

//写入int32
func WriteInt32(val int32, buf *[]byte, pos int32) (count int32) {
	if len(*buf) < int(pos)+4 {
		*buf = append(*buf, make([]byte, 4)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}
	(*buf)[pos] = byte(val)
	(*buf)[pos+1] = byte(val >> 8)
	(*buf)[pos+2] = byte(val >> 16)
	(*buf)[pos+3] = byte(val >> 24)

	return 4
}

//写入int64
func WriteInt64(val int64, buf *[]byte, pos int32) (count int32) {
	if len(*buf) < int(pos)+8 {
		*buf = append(*buf, make([]byte, 8)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}

	(*buf)[pos] = byte(val)
	(*buf)[pos+1] = byte(val >> 8)
	(*buf)[pos+2] = byte(val >> 16)
	(*buf)[pos+3] = byte(val >> 24)

	(*buf)[pos+4] = byte(val >> 32)
	(*buf)[pos+5] = byte(val >> 40)
	(*buf)[pos+6] = byte(val >> 48)
	(*buf)[pos+7] = byte(val >> 56)

	return 8
}

//写入CString
func WriteCString(val string, buf *[]byte, pos int32) (count int32) {
	bval := []byte(val)
	count = int32(len(bval)) + 1
	if len(*buf) < int(pos+count) {
		*buf = append(*buf, make([]byte, count)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}

	copy((*buf)[pos:], bval)
	(*buf)[pos+count-1] = 0

	return
}

//注入length和request id
func InjectHead(buf *[]byte, length int32, reqId int32) {
	WriteInt32(length, buf, 0)
	WriteInt32(reqId, buf, 4)
}
