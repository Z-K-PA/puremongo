package wire_protocol

//固定格式的消息

var queryMetaMsg *QueryMsg

func init() {
	queryMetaMsg = NewQueryMsg()
	//设置为系统表
	queryMetaMsg.FullCollName = "admin.$cmd"
	//什么查询flag都不用设置
	queryMetaMsg.Flags = 0
	//返回值填-1
	queryMetaMsg.NumToReturn = -1
}
