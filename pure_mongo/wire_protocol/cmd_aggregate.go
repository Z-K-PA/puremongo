package wire_protocol

//Aggregation参数
type AggregateOption struct {
	CollectionName string        `bson:"aggregate"`
	Db             string        `bson:"$db"`
	Pipeline       []interface{} `bson:"pipeline"`
	AllowDiskUse   bool          `bson:"allowDiskUse"`
	Cursor         interface{}   `bson:"cursor"`
}

//Aggregation参数带超时
type AggregateOptionWithTimeout struct {
	AggregateOption `bson:",inline"`
	MaxTimeMS       int32 `bson:"maxTimeMS"`
}
