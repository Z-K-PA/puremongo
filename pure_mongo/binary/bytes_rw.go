package binary

import (
	"bytes"
	"errors"
)

const (
	ResizeBuf = 256 * 1024 // 256k
)

var (
	ErrInvalidBinary = errors.New("错误的二进制格式")
)

//读取byte
func ReadByte(buf []byte, pos int32) (val byte, err error) {
	if len(buf) < int(pos)+1 {
		err = ErrInvalidBinary
		return
	}
	val = buf[pos]
	return
}

//读取int32
func ReadInt32(buf []byte, pos int32) (val int32, err error) {
	if len(buf) < int(pos)+4 {
		err = ErrInvalidBinary
		return
	}
	val = (int32(buf[pos])) | (int32(buf[pos+1]) << 8) | (int32(buf[pos+2]) << 16) | (int32(buf[pos+3]) << 24)
	return
}

//读取int64
func ReadInt64(buf []byte, pos int32) (val int64, err error) {
	if len(buf) < int(pos)+8 {
		err = ErrInvalidBinary
		return
	}
	val = (int64(buf[pos])) | (int64(buf[pos+1]) << 8) | (int64(buf[pos+2]) << 16) | (int64(buf[pos+3]) << 24) |
		(int64(buf[pos+4]) << 32) | (int64(buf[pos+5]) << 40) | (int64(buf[pos+6]) << 48) | (int64(buf[pos+7]) << 56)
	return
}

//读取CString
func ReadCString(buf []byte, pos int32) (val string, cstringLen int32, err error) {
	if len(buf) < int(pos)+1 {
		//空串也至少需要一个字节
		err = ErrInvalidBinary
	}

	nullPos := bytes.IndexByte(buf[pos:], 0)
	if nullPos == -1 {
		err = ErrInvalidBinary
		return
	}
	_nullPos := int32(nullPos)
	cstringLen = _nullPos + 1
	val = string(buf[pos : pos+_nullPos])
	return
}

//重置缓存
func resizeBuffer(buf *[]byte, needSize int) bool {
	if len(*buf) < needSize {
		oldBuf := *buf
		*buf = make([]byte, needSize+ResizeBuf)
		copy(*buf, oldBuf)
		return true
	}
	return false
}

//写入byte
func WriteByte(val byte, buf *[]byte, pos int32) (count int32) {
	/*if len(*buf) < int(pos)+1 {
		*buf = append(*buf, val)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	} else {
		(*buf)[pos] = byte(val)
	}*/

	needSize := int(pos) + 1
	resizeBuffer(buf, needSize)

	(*buf)[pos] = val

	return 1
}

//写入int32
func WriteInt32(val int32, buf *[]byte, pos int32) (count int32) {
	/*if len(*buf) < int(pos)+4 {
		*buf = append(*buf, make([]byte, 4)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}
	(*buf)[pos] = byte(val)
	(*buf)[pos+1] = byte(val >> 8)
	(*buf)[pos+2] = byte(val >> 16)
	(*buf)[pos+3] = byte(val >> 24)*/

	needSize := int(pos) + 4
	resizeBuffer(buf, needSize)

	(*buf)[pos] = byte(val)
	(*buf)[pos+1] = byte(val >> 8)
	(*buf)[pos+2] = byte(val >> 16)
	(*buf)[pos+3] = byte(val >> 24)

	return 4
}

//写入int64
func WriteInt64(val int64, buf *[]byte, pos int32) (count int32) {
	/*if len(*buf) < int(pos)+8 {
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
	(*buf)[pos+7] = byte(val >> 56)*/

	needSize := int(pos) + 8
	resizeBuffer(buf, needSize)

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

	/*if len(*buf) < int(pos+count) {
		*buf = append(*buf, make([]byte, count)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}

	copy((*buf)[pos:], bval)
	(*buf)[pos+count-1] = 0*/

	needSize := int(pos + count)
	resizeBuffer(buf, needSize)

	copy((*buf)[pos:], bval)
	(*buf)[pos+count-1] = 0

	return
}

//只在分配内存不够时追加字节切片到另一个切片
func AppendBytesIfNeed(buf *[]byte, out []byte, pos int32) (bsonLen int32) {
	bsonLen = int32(len(out))
	length := int(pos + bsonLen)

	/*if cap(*buf) < length {
		//内存不够，需要重新分配
		*buf = append(*buf, out...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}*/

	if resizeBuffer(buf, length) {
		copy((*buf)[pos:], out)
	}

	return
}

//注入length和request id
func InjectHead(buf *[]byte, length int32, reqId int32) {
	WriteInt32(length, buf, 0)
	WriteInt32(reqId, buf, 4)
}
