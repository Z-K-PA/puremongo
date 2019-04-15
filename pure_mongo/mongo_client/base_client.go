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
)

//Mongo客户端
type BaseMongoClient struct {
	//连接
	conn net.Conn
	//缓存
	buffer []byte
	//递增的reqId
	reqId int32
	//事务序号
	txnId int64
	//事务id
	sessionId uuid.UUID
	//消息头
	msgHeader wire_protocol.MsgHeader
	//此连接是否有问题，能否继续使用
	badSmell bool
	//此连接是否使用过
	used bool

	//此锁只需要对连接操作加锁即可
	closed    bool
	closeLock sync.Mutex
}

//新建连接
func newMongoClient(conn net.Conn, sessionId uuid.UUID) *BaseMongoClient {
	cli := &BaseMongoClient{
		conn:      conn,
		buffer:    make([]byte, ClientBufferInitSize),
		sessionId: sessionId,
	}
	return cli
}

//拨号连接
func dialMongoClient(ctx context.Context, dialer *net.Dialer, url string) (cli *BaseMongoClient, err error) {
	var conn net.Conn
	var sessionId uuid.UUID

	select {
	case <-ctx.Done():
		//显式关闭连接
		cli.Close()
		err = ctx.Err()
		return
	default:
	}
	conn, err = dialer.DialContext(ctx, "tcp", url)
	if err != nil {
		return
	}
	sessionId, err = uuid.New()
	if err != nil {
		return
	}
	cli = newMongoClient(conn, sessionId)
	return
}

//关闭
func (cli *BaseMongoClient) Close() (err error) {
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
func (cli *BaseMongoClient) IsClosed() (closed bool) {
	cli.closeLock.Lock()
	closed = cli.closed
	cli.closeLock.Unlock()
	return
}

//是否可以继续使用
func (cli *BaseMongoClient) HasBadSmell() bool {
	return cli.badSmell
}

//此连接是否使用过
func (cli *BaseMongoClient) HasUsed() bool {
	return cli.used
}

//重置缓冲区
func (cli *BaseMongoClient) ResetBuffer() {
	l := len(cli.buffer)

	if l == 0 || l > ClientBufferLimitSize {
		cli.buffer = make([]byte, ClientBufferInitSize)
	}
}

//超时设置
func (cli *BaseMongoClient) setDeadline(ctx context.Context) (err error) {
	deadLine, ok := ctx.Deadline()
	if ok {
		//带超时 -- 直接设置超时，如果超时时间已经超过当前时间，也不用管，net.Conn => Read/Write会处理好这种情况
		err = cli.conn.SetDeadline(deadLine)
	}
	return
}

//发送字节流
func (cli *BaseMongoClient) _sendBuff(ctx context.Context, buf []byte) (err error) {
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
func (cli *BaseMongoClient) sendBuf(ctx context.Context, count int32) (reqId int32, err error) {
	if count > ClientSendMaxSize {
		err = ErrSendDataTooLarge
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

	//先加req
	cli.reqId++
	reqId = cli.reqId
	binary.InjectHead(&cli.buffer, count, reqId)

	err = cli._sendBuff(ctx, cli.buffer[:count])
	return
}

//接收字节流
func (cli *BaseMongoClient) readBuf(ctx context.Context) (err error) {
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

//发送字节流后接收字节流
func (cli *BaseMongoClient) sendAndRecv(ctx context.Context, count int32) (err error) {
	reqId := int32(0)

	//设置connection超时
	err = cli.setDeadline(ctx)
	if err != nil {
		cli.badSmell = true
		return
	}

	//发送字节流
	reqId, err = cli.sendBuf(ctx, count)
	if err != nil {
		cli.badSmell = true
		return
	}

	//接收字节流
	err = cli.readBuf(ctx)
	if err != nil {
		cli.badSmell = true
		return
	}

	//请求回应id不匹配
	if reqId != cli.msgHeader.ResTo {
		cli.badSmell = true
		err = wire_protocol.ErrInvalidMsgFromSrv
		return
	}
	return
}

//发送一个query,接收一个reply
func (cli *BaseMongoClient) sendQueryRecvReply(ctx context.Context, qMsg *wire_protocol.QueryMsg) (
	rMsg *wire_protocol.ReplyMsg, err error) {
	count := int32(0)
	//先序列化，如果序列化出错，返回，但连接还可以用
	count, err = qMsg.Marshal(&cli.buffer)
	if err != nil {
		return
	}
	if count > ClientSendMaxSize {
		err = ErrSendDataTooLarge
		return
	}

	err = cli.sendAndRecv(ctx, count)
	if err != nil {
		return
	}

	rMsg, err = wire_protocol.ParseReplyMsg(cli.msgHeader, cli.buffer[:cli.msgHeader.MsgLen])
	if err != nil {
		cli.badSmell = true
	}
	return
}

//发送一个enhance msg,接收一个enhance msg
func (cli *BaseMongoClient) enhanceSendMsgRecvMsg(ctx context.Context, inMsg *wire_protocol.EnhanceMsg) (
	outMsg *wire_protocol.EnhanceMsg, err error) {
	count := int32(0)
	//先序列化，如果序列化出错，返回，但连接还可以用
	count, err = inMsg.Marshal(&cli.buffer)
	if err != nil {
		return
	}
	if count > ClientSendMaxSize {
		err = ErrSendDataTooLarge
		return
	}

	err = cli.sendAndRecv(ctx, count)
	if err != nil {
		return
	}

	outMsg, err = wire_protocol.ParseEnhanceMsg(cli.msgHeader, cli.buffer[:cli.msgHeader.MsgLen])
	if err != nil {
		cli.badSmell = true
	}
	return
}

//发送传入的字节流
func (cli *BaseMongoClient) sendSpecBuf(ctx context.Context, buf []byte) (reqId int32, err error) {
	count := int32(len(buf))
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
	reqId = cli.reqId
	binary.InjectHead(&buf, count, reqId)

	err = cli._sendBuff(ctx, buf)
	return
}

//发送字节流后接收字节流
func (cli *BaseMongoClient) sendSpecBufAndRecv(ctx context.Context, buf []byte) (err error) {
	reqId := int32(0)

	//设置connection超时
	err = cli.setDeadline(ctx)
	if err != nil {
		cli.badSmell = true
		return
	}

	//发送字节流
	reqId, err = cli.sendSpecBuf(ctx, buf)
	if err != nil {
		cli.badSmell = true
		return
	}

	//接收字节流
	err = cli.readBuf(ctx)
	if err != nil {
		cli.badSmell = true
		return
	}

	//请求回应id不匹配
	if reqId != cli.msgHeader.ResTo {
		cli.badSmell = true
		err = wire_protocol.ErrInvalidMsgFromSrv
		return
	}
	return
}

//发送固定字节流,接收一个reply
func (cli *BaseMongoClient) sendQueryBufRecvReply(ctx context.Context, buf []byte) (
	rMsg *wire_protocol.ReplyMsg, err error) {
	err = cli.sendSpecBufAndRecv(ctx, buf)
	if err != nil {
		return
	}
	rMsg, err = wire_protocol.ParseReplyMsg(cli.msgHeader, cli.buffer[:cli.msgHeader.MsgLen])
	if err != nil {
		cli.badSmell = true
	}
	return
}

//处理查询消息
func (cli *BaseMongoClient) queryWithHandler(ctx context.Context,
	qMsg *wire_protocol.QueryMsg, handler wire_protocol.HandleBsonDoc) (err error) {
	var rMsg *wire_protocol.ReplyMsg

	rMsg, err = cli.sendQueryRecvReply(ctx, qMsg)
	if err != nil {
		return
	}
	for _, doc := range rMsg.Documents {
		err = handler(doc)
		if err != nil {
			cli.badSmell = true
			return
		}
	}
	return
}

//处理查询消息
func (cli *BaseMongoClient) queryBufWithHandler(ctx context.Context,
	buf []byte, handler wire_protocol.HandleBsonDoc) (err error) {
	var rMsg *wire_protocol.ReplyMsg

	rMsg, err = cli.sendQueryBufRecvReply(ctx, buf)
	if err != nil {
		return
	}
	for _, doc := range rMsg.Documents {
		err = handler(doc)
		if err != nil {
			cli.badSmell = true
			return
		}
	}
	return
}

//处理enhance消息
func (cli *BaseMongoClient) enhanceMsgWithHandler(ctx context.Context,
	inMsg *wire_protocol.EnhanceMsg,
	bodyHandler wire_protocol.HandleBsonDoc,
	seqDocHandler wire_protocol.HandleBsonDoc) (err error) {
	var outMsg *wire_protocol.EnhanceMsg

	outMsg, err = cli.enhanceSendMsgRecvMsg(ctx, inMsg)
	if err != nil {
		return
	}

	if bodyHandler != nil {
		err = bodyHandler(outMsg.Body.Doc)
		if err != nil {
			cli.badSmell = true
			return
		}
	}

	if seqDocHandler != nil {
		for _, doc := range outMsg.SeqDocList.DocList {
			err = seqDocHandler(doc)
			if err != nil {
				cli.badSmell = true
				return
			}
		}
	}

	return
}
