package connection

type LogicClient BaseMongoClient

func (cli *LogicClient) FindOne(collection string, filter struct{}, val interface{}) (err error) {
	return
}

func (cli *LogicClient) InsertOne(collection string, val interface{}) (err error) {
	return
}