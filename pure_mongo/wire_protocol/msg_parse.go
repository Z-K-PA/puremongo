package wire_protocol

//解析API Msg
func ParseAPIMsg(header MsgHeader, buf []byte) (eMsg *APIMsg, err error) {
	if header.OpCode != OpMsg {
		err = ErrInvalidMsgFromSrv
		return
	}

	eMsg = &APIMsg{}
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
