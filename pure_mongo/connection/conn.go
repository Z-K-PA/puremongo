package connection

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
	"io"
	"net"
	"pure_mongos/pure_mongo/binary"
	"pure_mongos/pure_mongo/wire_protocol"
	"sync"
)

const (
	/*
	* 如果每台机器上有100个连接，则一般情况下buffer的内存消耗为100M
	* 如果某个连接超过4M，则在此连接完成一个回合的消息后，将buffer重置为1M
	* 一个连接是回合制，即先Write->服务器，再Read<-服务器
	* 连接给一个go程使用，所以此buffer可以为序列化，读，写套接字复用
	* 极端情况下100个连接可能会超过800M
	 */
	//给每一个连接初始的buffer大小
	ClientBufferInitSize = 1024 * 1024
	//在每个回合结束后来查看client buffer大小，如果大于8M就重置为1M
	ClientBufferLimitSize = 8 * 1024 * 1024

	//一个回合中连接最多的接收数据大小
	ClientReceiveMaxSize = 40 * 1024 * 1024 //40M，很变态的数字了
	//一个回合中连接最多的发送数据大小
	ClientSendMaxSize = 40 * 1024 * 1024
)

var (
	ErrSendDataTooLarge = errors.New("发送的消息太大")
	ErrRevDataTooLarge  = errors.New("接收的消息太大")
	ErrMarshalError = errors.New("消息序列化失败")
)

type MongoClient struct {
	conn      net.Conn
	buffer    []byte
	reqId     int32
	txnId     int64
	sessionId uuid.UUID
	msgHeader wire_protocol.MsgHeader

	//此锁只需要对连接操作加锁即可
	closed    bool
	closeLock sync.Mutex
}

func NewMongoClient(conn net.Conn, uuid uuid.UUID) *MongoClient {
	cli := &MongoClient{
		conn:      conn,
		buffer:    make([]byte, ClientBufferInitSize),
		sessionId: uuid,
	}
	return cli
}

//关闭
func (cli *MongoClient) Close() (err error) {
	cli.closeLock.Lock()
	if cli.closed {
		cli.closeLock.Unlock()
		return
	}
	err = cli.conn.Close()
	cli.closeLock.Unlock()
	return
}

//是否关闭
func (cli *MongoClient) IsClosed() (closed bool) {
	cli.closeLock.Lock()
	closed = cli.closed
	cli.closeLock.Unlock()
	return
}

//重置缓冲区
func (cli *MongoClient) ResetBuffer() {
	l := len(cli.buffer)

	if l == 0 || l > ClientBufferLimitSize {
		cli.buffer = make([]byte, ClientBufferInitSize)
	}
}

//超时设置
func (cli *MongoClient) setDeadline(ctx context.Context) (err error) {
	deadLine, ok := ctx.Deadline()
	if ok {
		//带超时 -- 直接设置超时，如果超时时间已经超过当前时间，也不用管，net.Conn => Read/Write会处理好这种情况
		err = cli.conn.SetDeadline(deadLine)
	}
	return
}

//发送字节流
func (cli *MongoClient) _sendBuff(ctx context.Context, buf []byte) (err error) {
	size := len(buf)
	writeSize := 0
	index := 0

	for {
		select {
		case <-ctx.Done():
			//显式关闭连接
			cli.Close()
			err = ctx.Err()
			return
		default:
		}

		writeSize, err = cli.conn.Write(buf[index:])
		if err != nil {
			return err
		}
		index += writeSize
		if index == size {
			return
		}
	}
}

//发送字节流
func (cli *MongoClient) sendBuf(ctx context.Context, count int32) (err error) {
	select {
	case <-ctx.Done():
		//显式关闭连接
		cli.Close()
		err = ctx.Err()
		return
	default:
	}

	if count > ClientSendMaxSize {
		err = ErrSendDataTooLarge
		return
	}

	//先加req
	cli.reqId++
	binary.InjectHead(&cli.buffer, count, cli.reqId)

	err = cli._sendBuff(ctx, cli.buffer[:count])
	return
}

//接收字节流
func (cli *MongoClient) readBuf(ctx context.Context) (err error) {
	select {
	case <-ctx.Done():
		//显式关闭连接
		cli.Close()
		err = ctx.Err()
		return
	default:
	}

	//读取头部
	headBuf := cli.buffer[:wire_protocol.HeaderLen]
	_, err = io.ReadFull(cli.conn, headBuf)
	if err != nil {
		return
	}
	cli.msgHeader.Read(headBuf)
	if cli.msgHeader.MsgLen > ClientReceiveMaxSize {
		err = ErrRevDataTooLarge
		return
	}

	select {
	case <-ctx.Done():
		//显式关闭连接
		cli.Close()
		err = ctx.Err()
		return
	default:
	}

	delta := int(cli.msgHeader.MsgLen) - len(cli.buffer)
	if delta > 0 {
		//buffer不够
		cli.buffer = append(cli.buffer, make([]byte, delta)...)
		//扩大buf
		cli.buffer = (cli.buffer)[:cap(cli.buffer)]
	}
	_, err = io.ReadFull(cli.conn, cli.buffer[wire_protocol.HeaderLen:cli.msgHeader.MsgLen])
	return
}

//一个回合
func (cli *MongoClient) OnceRound(ctx context.Context, msg wire_protocol.IReqMsg) (err error) {
	count := int32(0)
	count, err = msg.Marshal(&cli.buffer)
	if err != nil {
		return err
	}

	err = cli.setDeadline(ctx)
	if err != nil {
		return
	}

}
