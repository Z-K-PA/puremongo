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

//写入int32
func WriteInt32(val int32, buf []byte, pos int32) {
	buf[pos] = byte(val)
	buf[pos+1] = byte(val >> 8)
	buf[pos+2] = byte(val >> 16)
	buf[pos+3] = byte(val >> 24)
}

//读取int64
func ReadInt64(buf []byte, pos int32) int64 {
	return (int64(buf[pos])) | (int64(buf[pos+1]) << 8) | (int64(buf[pos+2]) << 16) | (int64(buf[pos+3]) << 24) |
		(int64(buf[pos+4]) << 32) | (int64(buf[pos+5]) << 40) | (int64(buf[pos+6]) << 48) | (int64(buf[pos+7]) << 56)
}

//写入int64
func WriteInt64(val int64, buf []byte, pos int32) {
	buf[pos] = byte(val)
	buf[pos+1] = byte(val >> 8)
	buf[pos+2] = byte(val >> 16)
	buf[pos+3] = byte(val >> 24)

	buf[pos+4] = byte(val >> 32)
	buf[pos+5] = byte(val >> 40)
	buf[pos+6] = byte(val >> 48)
	buf[pos+7] = byte(val >> 56)
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

//写入CString
func WriteCString(val string, buf []byte, pos int32) (cstringLen int32) {
	bval := []byte(val)
	cstringLen = int32(len(bval)) + 1
	copy(buf[pos:], bval)
	buf[pos+cstringLen] = 0
	return
}

//注入length和request id
func InjectHead(buf []byte, reqId int32) {
	length := int32(len(buf))
	WriteInt32(length, buf, 0)
	WriteInt32(reqId, buf, 4)
}
