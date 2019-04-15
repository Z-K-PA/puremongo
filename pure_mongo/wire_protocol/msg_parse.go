package wire_protocol

import "pure_mongos/pure_mongo/bson"

//解析EnhanceMsg
func ParseEnhanceMsg(header MsgHeader, buf []byte) (eMsg *EnhanceMsg, err error) {
	if header.OpCode != OpMsg {
		err = ErrInvalidMsgFromSrv
		return
	}

	eMsg = &EnhanceMsg{}
	err = eMsg.FromBuffer(header, buf)
	return
}

//解析ReplyMsg
func ParseReplyMsg(header MsgHeader, buf []byte) (qMsg *ReplyMsg, err error) {
	if header.OpCode != OpReply {
		err = ErrInvalidMsgFromSrv
		return
	}
	qMsg = &ReplyMsg{}
	err = qMsg.FromBuffer(header, buf)
	return
}

//处理bson doc
type HandleBsonDoc func(doc bson.BsonDoc) (err error)
