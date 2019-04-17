package connection

import (
	"context"
	"errors"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrNotFound       = errors.New("没有查询到数据")
	ErrFindCmdInvalid = errors.New("查询命令不正确")
	ErrFindDataErr    = errors.New("插入数据后服务端返回数据格式错误")
)

type MongoFindClient struct {
	*MongoClient
	options  wire_protocol.FindOption //查询参数
	cursorId int64                    //对应的服务器游标
}

//做个包装 -- db, collection, filter (此3项必填)
func (cli *MongoClient) Find(db string, collection string, filter map[string]interface{}) *MongoFindClient {
	findCli := &MongoFindClient{
		MongoClient: cli,
	}
	findCli.options.Db = db
	findCli.options.CollectionName = collection
	findCli.options.Filter = filter
	return findCli
}

//写链式调用 - sort (排序)
func (cli *MongoFindClient) Sort(sort interface{}) *MongoFindClient {
	cli.options.SortVal = sort
	return cli
}

//写链式调用 - select (选出field返回)
func (cli *MongoFindClient) Select(projection interface{}) *MongoFindClient {
	cli.options.Projection = projection
	return cli
}

//写链式调用 - skip（跳过cursor）
func (cli *MongoFindClient) Skip(skip int32) *MongoFindClient {
	cli.options.SkipVal = skip
	return cli
}

//写链式调用-limit （限定返回值）
func (cli *MongoFindClient) Limit(limit int32) *MongoFindClient {
	cli.options.LimitVal = limit
	return cli
}

//写链式调用-maxTimeMS （如果服务在指定的毫秒数内没有查询到相关的数据，则中断查询）
func (cli *MongoFindClient) MaxTimeMS(maxTimeMS int32) *MongoFindClient {
	cli.options.MaxTimeMSVal = maxTimeMS
	return cli
}

//设置查询参数
func (cli *MongoFindClient) getFindMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(cli.options)
	return findMsg
}

//设置分批查询参数
func (cli *MongoFindClient) getMoreMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	if cli.options.MaxTimeMSVal != 0 {
		findMsg.SetBodyDoc(wire_protocol.GetMoreWithTimeout{
			CursorId:       cli.cursorId,
			CollectionName: cli.options.CollectionName,
			MaxTimeMS:      cli.options.MaxTimeMSVal,
		})
	} else {
		findMsg.SetBodyDoc(wire_protocol.GetMore{
			CursorId:       cli.cursorId,
			CollectionName: cli.options.CollectionName,
		})
	}
	return findMsg
}

//设置关闭cursor参数
func (cli *MongoFindClient) getCloseCursorMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(wire_protocol.CursorKill{
		Db:             cli.options.Db,
		CollectionName: cli.options.CollectionName,
		CursorList:     []int64{cli.cursorId},
	})
	return findMsg
}

//检查命令参数
func (cli *MongoFindClient) verifyParam() error {
	if cli.options.Db == "" || cli.options.CollectionName == "" || cli.options.Filter == nil {
		return ErrFindCmdInvalid
	} else {
		return nil
	}
}

//单个文档的查询
func (cli *MongoFindClient) One(ctx context.Context, val interface{}) (err error) {
	err = cli.verifyParam()
	if err != nil {
		return
	}

	cli.options.SingleBatch = true
	cli.options.LimitVal = 1
	inMsg := cli.getFindMsg()

	unmarshalfunc := func(buf []byte) error {
		return bson.UnMarshalBson(buf, val)
	}
	err = cli.MongoClient.RunAPIMsgWithBodyBinaryHandler(ctx, inMsg, unmarshalfunc)
	return
}
