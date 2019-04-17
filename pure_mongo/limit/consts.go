package limit

const (
	//bson单个文档最大设定为16M
	MaxBsonDocSize = 16777216
	//一个回合中连接最多的接收数据大小
	ClientReceiveMaxSize = 48000000 * 2
	//一个回合中连接最多的发送数据大小
	ClientSendMaxSize = 48000000
	//一个回合中最多的文档数量
	MaxBatchSize = 100000
)
