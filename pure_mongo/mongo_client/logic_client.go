package connection

type LogicClient struct {
	*BaseMongoClient
}

func (cli *LogicClient) FindOne(dbName string, filter struct{}, val interface{}) (err error) {
	return
}
