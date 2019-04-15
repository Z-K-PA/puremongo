package wire_protocol

type InsertMeta struct {
	CollectionName string `bson:"insert"`
	Ordered        bool   `bson:"ordered"`
	Db             string `bson:"$db"`
}

func NewInsertOneMessage(db string, collection string, ordered bool, item interface{}) *EnhanceMsg {
	enMsg := NewEnhanceMsg()
	enMsg.SetBodyDoc(InsertMeta{
		Db:             db,
		CollectionName: collection,
		Ordered:        ordered,
	})
	enMsg.SetSeqDoc("insert.documents")
	enMsg.AddSeqDoc(item)
	return enMsg
}
