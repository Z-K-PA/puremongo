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
