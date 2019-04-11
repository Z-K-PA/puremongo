package connection

import (
	"errors"
	"io"
	"net"
	"pure_mongos/pure_mongo/wire_protocol"
	"time"
)

var (
	ErrConnTimeoutBefore = errors.New("开始连接Mongo服务器前已经超时")
)

//连接服务器
func DialMongo(url string, deadline time.Time) (conn net.Conn, err error) {
	now := time.Now()

	//开始连接前检查是否已经超时
	timeout := now.Sub(deadline)
	if timeout <= 0 {
		err = ErrConnTimeoutBefore
		return
	}

	conn, err = net.DialTimeout("tcp", url, timeout)
	return
}

//发送消息
func SendMsg(conn net.Conn, buf []byte, deadline time.Time) (err error) {
	size := len(buf)
	writeSize := 0
	index := 0

	err = conn.SetDeadline(deadline)
	if err != nil {
		return
	}

	for {
		writeSize, err = conn.Write(buf[index:])
		if err != nil {
			return err
		}
		index += writeSize
		if index == size {
			return
		}
	}
}

//接收消息
func ReadMsg(conn net.Conn, buf []byte, deadline time.Time) (msgHeader wire_protocol.MsgHeader, rawData []byte, err error) {
	//设置超时
	err = conn.SetDeadline(deadline)
	if err != nil {
		return
	}

	rawData = buf
	//读取头部
	headBuf := buf[:wire_protocol.HeaderLen]
	_, err = io.ReadFull(conn, headBuf)
	if err != nil {
		return
	}

	msgHeader.Read(headBuf)
	if len(rawData) < int(msgHeader.MsgLen) {
		rawData = make([]byte, msgHeader.MsgLen)
		//拷贝头
		copy(rawData, headBuf)
	}

	dataBuf := rawData[wire_protocol.HeaderLen:]
	_, err = io.ReadFull(conn, dataBuf)
	return
}
