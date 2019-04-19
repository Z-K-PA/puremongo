package connection

import (
	"context"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/wire_protocol"
)

//处理单个文档的查询结果并将结果序列化
func handleOnceFetch(ctx context.Context, cli IFetchMongoClient, val interface{}) (err error) {
	var findResult *wire_protocol.FindResult

	err = cli.VerifyParam()
	if err != nil {
		return
	}

	cli.SetOnce()

	inMsg := cli.GetFindMsg()

	findResult, err = cli.GetMongoClient().runFetchCmd(ctx, inMsg, "firstBatch")
	if err != nil {
		return
	}

	if findResult.OK == 0 {
		err = ErrFindDataErr
		return
	}

	if len(findResult.DocList) > 0 {
		err = bson.UnMarshalBson(findResult.DocList[0], val)
	} else {
		err = ErrNotFound
	}
	return
}

//处理多个文档的查询
func handleIterFetch(ctx context.Context, cli IFetchMongoClient) (cursor *Cursor, err error) {
	var findResult *wire_protocol.FindResult
	err = cli.VerifyParam()
	if err != nil {
		return
	}

	inMsg := cli.GetFindMsg()

	findResult, err = cli.GetMongoClient().runFetchCmd(ctx, inMsg, "firstBatch")
	if err != nil {
		return
	}

	if findResult.OK == 0 {
		err = ErrFindDataErr
		return
	}

	cursor = newCursor(cli, findResult, cli.options.Db, cli.options.CollectionName, cli.options.MaxTimeMS)
	return
}
