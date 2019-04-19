package connection

import (
	"context"
	"errors"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrAggregateCmdInvalid = errors.New("aggregate命令不正确")
)

type MongoAggregateClient struct {
	*MongoClient
	options *wire_protocol.AggregateOptionWithTimeout
}

//做个包装 -- db, collection(此2项必填)
func (cli *MongoClient) Aggregate(db string, collection string) *MongoAggregateClient {
	findCli := &MongoAggregateClient{
		MongoClient: cli,
		options:     &wire_protocol.AggregateOptionWithTimeout{},
	}
	findCli.options.Db = db
	findCli.options.CollectionName = collection
	findCli.options.Pipeline = make([]interface{}, 0, 4)
	return findCli
}

//实现IFetchMongoClient接口
//获取实际的client
func (cli *MongoAggregateClient) GetMongoClient() *MongoClient {
	return cli.MongoClient
}

//获取查询消息
func (cli *MongoAggregateClient) GetFindMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(cli.options)
	return findMsg
}

//检查命令参数
func (cli *MongoAggregateClient) VerifyParam() error {
	if cli.options.Db == "" || cli.options.CollectionName == "" || len(cli.options.Pipeline) == 0 {
		return ErrAggregateCmdInvalid
	} else {
		if cli.options.Cursor == nil {
			cli.options.Cursor = map[string]interface{}{}
		}
		return nil
	}
}

//设置单次查询
func (cli *MongoAggregateClient) SetOnce() {
	//加上limit = 1的限制
	cli.Pipeline(map[string]interface{}{"limit": 1})
}

//写链式调用 -- Pipeline
func (cli *MongoAggregateClient) Pipeline(pipeline interface{}) *MongoAggregateClient {
	if pipeline != nil {
		cli.options.Pipeline = append(cli.options.Pipeline, pipeline)
	}
	return cli
}

//写链式调用 -- AllowDiskUse -- pipeline过程中是否可以生成临时文件
func (cli *MongoAggregateClient) AllowDiskUse() *MongoAggregateClient {
	cli.options.AllowDiskUse = true
	return cli
}

//写链式调用 -- 设置Cursor
func (cli *MongoAggregateClient) Cursor(cursor interface{}) *MongoAggregateClient {
	if cursor != nil {
		cli.options.Cursor = cursor
	}
	return cli
}

//写链式调用 -- 设置MaxTimeMS
func (cli *MongoAggregateClient) MaxTimeMS(maxTimeMs int32) *MongoAggregateClient {
	cli.options.MaxTimeMS = maxTimeMs
	return cli
}

//单个文档的查询
func (cli *MongoAggregateClient) One(ctx context.Context, val interface{}) (err error) {
	return handleOnceFetch(ctx, cli, val)
}

//多个文档的查询
func (cli *MongoAggregateClient) Iter(ctx context.Context) (cursor *Cursor, err error) {
	var findResult *wire_protocol.FindResult
	err = cli.VerifyParam()
	if err != nil {
		return
	}

	inMsg := cli.GetFindMsg()

	findResult, err = cli.MongoClient.runFetchCmd(ctx, inMsg, "firstBatch")
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
