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
	ErrConnToMuchData    = errors.New("服务器疯掉，发了过量数据")
)

const (
	ReceiveMaxSize = 50 * 1024 * 1024 //50M，很变态的数字了
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
func ReadMsg(conn net.Conn, buf *[]byte, deadline time.Time) (msgHeader wire_protocol.MsgHeader, err error) {
	//设置超时
	err = conn.SetDeadline(deadline)
	if err != nil {
		return
	}

	//读取头部
	headBuf := (*buf)[:wire_protocol.HeaderLen]
	_, err = io.ReadFull(conn, headBuf)
	if err != nil {
		return
	}

	msgHeader.Read(headBuf)

	delta := int(msgHeader.MsgLen) - len(*buf)
	if delta > ReceiveMaxSize {
		err = ErrConnToMuchData
		return
	}

	if delta > 0 {
		*buf = append(*buf, make([]byte, delta)...)
		//扩大buf
		*buf = (*buf)[:cap(*buf)]
	}

	dataBuf := (*buf)[wire_protocol.HeaderLen:int(msgHeader.MsgLen)]
	_, err = io.ReadFull(conn, dataBuf)
	return
}
