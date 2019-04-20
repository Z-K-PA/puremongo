package connection

import (
	"context"
	"errors"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrNotFound       = errors.New("没有查询到数据")
	ErrFindCmdInvalid = errors.New("查询命令不正确")

	ErrFindDataErr     = errors.New("查询数据后服务端返回数据格式错误")
	ErrFindMoreDataErr = errors.New("获取更多查询数据后服务端返回数据格式错误")

	ErrKillCursorCmdInvalid = errors.New("注销cursor后服务端返回数据格式错误")
)

type MongoFindClient struct {
	*MongoClient
	options *wire_protocol.FindOption //查询参数
}

//做个包装 -- db, collection, filter (此3项必填)
func (cli *MongoClient) Find(db string, collection string, filter interface{}) *MongoFindClient {
	findCli := &MongoFindClient{
		MongoClient: cli,
		options:     &wire_protocol.FindOption{},
	}
	findCli.options.Db = db
	findCli.options.CollectionName = collection
	findCli.options.Filter = filter
	return findCli
}

//实现IFetchMongoClient接口
//获取实际的client
func (cli *MongoFindClient) GetMongoClient() *MongoClient {
	return cli.MongoClient
}

//获取查询消息
func (cli *MongoFindClient) GetFindMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(cli.options)
	return findMsg
}

//检查命令参数
func (cli *MongoFindClient) VerifyParam() error {
	if cli.options.Db == "" || cli.options.CollectionName == "" || cli.options.Filter == nil {
		return ErrFindCmdInvalid
	} else {
		return nil
	}
}

//设置单次查询
func (cli *MongoFindClient) SetOnce() {
	cli.options.SingleBatch = true
	cli.options.Limit = 1
}

//获取db name
func (cli *MongoFindClient) GetDbName() string {
	return cli.options.Db
}

//获取collection name
func (cli *MongoFindClient) GetCollectionName() string {
	return cli.options.CollectionName
}

//获取maxTimeMS
func (cli *MongoFindClient) GetMaxTimeMs() int32 {
	return cli.options.MaxTimeMS
}

//写链式调用 - sort (排序)
func (cli *MongoFindClient) Sort(sort interface{}) *MongoFindClient {
	cli.options.Sort = sort
	return cli
}

//写链式调用 - select (选出field返回)
func (cli *MongoFindClient) Projection(projection interface{}) *MongoFindClient {
	cli.options.Projection = projection
	return cli
}

//写链式调用 - skip（跳过cursor）
func (cli *MongoFindClient) Skip(skip int32) *MongoFindClient {
	cli.options.Skip = skip
	return cli
}

//写链式调用-limit （限定返回值）
func (cli *MongoFindClient) Limit(limit int32) *MongoFindClient {
	cli.options.Limit = limit
	return cli
}

//写链式调用-maxTimeMS （如果服务在指定的毫秒数内没有查询到相关的数据，则中断查询）
func (cli *MongoFindClient) MaxTimeMS(maxTimeMS int32) *MongoFindClient {
	cli.options.MaxTimeMS = maxTimeMS
	return cli
}

//单个文档的查询
func (cli *MongoFindClient) One(ctx context.Context, val interface{}) (err error) {
	return handleOnceFetch(ctx, cli, val)
}

//多个文档的查询
func (cli *MongoFindClient) Iter(ctx context.Context) (cursor *Cursor, err error) {
	return handleIterFetch(ctx, cli)
}
