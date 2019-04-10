package wire_protocol

const (
	QueryFlagTailCursor      = 1 << 1
	QueryFlagSlaveOk         = 1 << 2
	QueryFlagOpLogReplay     = 1 << 3
	QueryFlagNoCursorTimeout = 1 << 4
	QueryFlagAwaitData       = 1 << 5
	QueryFlagExhaust         = 1 << 6
	QueryFlagPartial         = 1 << 7
)

type QueryMsg struct {
	Header       MsgHeader
	Flags        int32
	FullCollName string
	NumToSkip    int32
	NumToReturn  int32

	rawBuf []byte
}

func NewQueryMsg() *QueryMsg {
	qMsg := &QueryMsg{
		Header:MsgHeader{
			OpCode: OpQuery,
		},
	}
	return qMsg
}

//设置查询flag
func (qMsg *QueryMsg) SetFlag(flags ...int32) {
	//清空后再做
	qMsg.Flags = 0
	for _, flag := range flags {
		qMsg.Flags |= flag
	}
}
