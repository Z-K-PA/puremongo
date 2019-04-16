package wire_protocol

type FindMeta struct {
	CollectionName string      `bson:"find"`
	Filter         interface{} `bson:"filter"`
	Sort           interface{} `bson:"sort"`
	Projection     interface{} `bson:"projection"`

	Skip        int    `bson:"skip"`
	Limit       int    `bson:"limit"`
	BatchSize   int    `bson:"batchSize"`
	SingleBatch bool   `bson:"singleBatch"`
	Db          string `bson:"$db"`
}

type FindMetaWithTimeout struct {
	FindMeta  `bson:",inline"`
	MaxTimeMS int `bson:"maxTimeMS"`
}

//新建查询对象
func NewFindMessage(
	db string,
	collection string,
	filter interface{},
	sort interface{},
	projection interface{},
	skip int,
	limit int,
	batchSize int,
	singleBatch bool) {

}
