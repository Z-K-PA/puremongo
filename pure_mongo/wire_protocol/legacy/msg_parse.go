package legacy



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
