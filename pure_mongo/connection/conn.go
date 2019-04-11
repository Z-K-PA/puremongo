package connection

import "net"

const (
	/*
	* 如果每台机器上有100个连接，则一般情况下buffer的内存消耗为100M
	* 如果某个连接超过4M，则在此连接完成一个回合的消息后，将buffer重置为1M
	* 一个连接是回合制，即先Write->服务器，再Read<-服务器
	* 连接给一个go程使用，所以此buffer可以为序列化，读，写套接字复用
	*/
	ClientBufferSize = 1024*1024
	MaxBufferSize = 4*1024*1024
)

type MongoClient struct {
	Conn net.Conn
	Buffer []byte
	ReqId int32
	TxnId int64
}

func (client *MongoClient) ResetBuffer() {
	l := len(client.Buffer)

	if l==0 || l > MaxBufferSize {
		client.Buffer = make([]byte, ClientBufferSize)
	}
}


