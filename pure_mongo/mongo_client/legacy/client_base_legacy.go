package legacy

/* 暂时用不上，先注释
//处理API消息-带handler
func (cli *MongoClient) runAPIMsgWithHandler(
	ctx context.Context,
	inMsg *wire_protocol.APIMsg,
	bodyRspVal interface{},
	seqDocListHandler bson.UnmarshalDocListHandler,
	seqDocListVal interface{}) (err error) {

	var outMsg *wire_protocol.APIMsg
	outMsg, err = cli.sendAPIMsgRecvAPIMsg(ctx, inMsg)
	if err != nil {
		return
	}

	if bodyRspVal != nil {
		err = outMsg.Body.Doc.Unmarshal(bodyRspVal)
		if err != nil {
			cli.badSmell = true
			return
		}
	}

	if seqDocListHandler != nil && seqDocListVal != nil {
		err = outMsg.SeqDocList.DocList.UnmarshalWithHandler(seqDocListHandler, seqDocListVal)
		if err != nil {
			cli.badSmell = true
			return
		}
	}

	return
}
*/

//处理API消息-带 binary handler
/*func (cli *MongoClient) RunAPIMsgWithBodyBinaryHandler(
	ctx context.Context,
	inMsg *wire_protocol.APIMsg,
	bodyBinaryHandler bson.HandleBsonDocFunc) (err error) {

	var outMsg *wire_protocol.APIMsg
	outMsg, err = cli.sendAPIMsgRecvAPIMsg(ctx, inMsg)
	if err != nil {
		return
	}

	if bodyBinaryHandler != nil {
		err = bodyBinaryHandler(outMsg.Body.Doc.Buf)
		if err != nil {
			cli.badSmell = true
			return
		}
	}
	return
}
*/

/*
//处理查询消息
func (cli *MongoClient) query(
	ctx context.Context,
	qMsg *wire_protocol.QueryMsg,
	rspVal interface{}) (err error) {

	var rMsg *wire_protocol.ReplyMsg
	rMsg, err = cli.sendQueryRecvReply(ctx, qMsg)
	if err != nil {
		return
	}
	err = rMsg.Documents.Unmarshal(rspVal)
	if err != nil {
		cli.badSmell = true
		return
	}
	return
}

//处理查询消息
func (cli *MongoClient) queryBuf(
	ctx context.Context,
	buf []byte,
	rspVal interface{}) (err error) {

	var rMsg *wire_protocol.ReplyMsg
	rMsg, err = cli.sendQueryBufRecvReply(ctx, buf)
	if err != nil {
		return
	}
	err = rMsg.Documents.Unmarshal(rspVal)
	if err != nil {
		cli.badSmell = true
		return
	}
	return
}
*/

/*
//发送一个query,接收一个reply
func (cli *MongoClient) sendQueryRecvReply(ctx context.Context, qMsg *wire_protocol.QueryMsg) (
	rMsg *wire_protocol.ReplyMsg, err error) {
	count := int32(0)
	//先序列化，如果序列化出错，返回，但连接还可以用
	count, err = qMsg.MarshalBsonWithBuffer(&cli.buffer)
	if err != nil {
		return
	}
	if count > limit.ClientSendMaxSize {
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

//发送固定字节流,接收一个reply
func (cli *MongoClient) sendQueryBufRecvReply(ctx context.Context, buf []byte) (
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

//处理查询消息-带handler
func (cli *MongoClient) queryWithHandler(
	ctx context.Context,
	qMsg *wire_protocol.QueryMsg,
	rspHandler bson.UnmarshalDocListHandler,
	rspVal interface{}) (err error) {

	var rMsg *wire_protocol.ReplyMsg
	rMsg, err = cli.sendQueryRecvReply(ctx, qMsg)
	if err != nil {
		return
	}
	err = rMsg.Documents.UnmarshalWithHandler(rspHandler, rspVal)
	if err != nil {
		cli.badSmell = true
		return
	}
	return
}

//处理查询消息-带handler
func (cli *MongoClient) queryBufWithHandler(
	ctx context.Context,
	buf []byte,
	rspHandler bson.UnmarshalDocListHandler,
	rspVal interface{}) (err error) {

	var rMsg *wire_protocol.ReplyMsg
	rMsg, err = cli.sendQueryBufRecvReply(ctx, buf)
	if err != nil {
		return
	}
	err = rMsg.Documents.UnmarshalWithHandler(rspHandler, rspVal)
	if err != nil {
		cli.badSmell = true
		return
	}
	return
}

//发送传入的字节流
func (cli *MongoClient) sendSpecBuf(ctx context.Context, buf []byte) (reqId int32, err error) {
	count := int32(len(buf))
	select {
	case <-ctx.Done():
		//显式关闭连接
		cli.Close()
		err = ctx.Err()
		return
	default:
	}

	if count > limit.ClientSendMaxSize {
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
func (cli *MongoClient) sendSpecBufAndRecv(ctx context.Context, buf []byte) (err error) {
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
*/
