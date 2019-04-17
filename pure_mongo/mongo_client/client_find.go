package connection

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/bson/mongo_driver_bson"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrNotFound       = errors.New("没有查询到数据")
	ErrFindCmdInvalid = errors.New("查询命令不正确")
	ErrFindDataErr    = errors.New("插入数据后服务端返回数据格式错误")
)

type MongoFindClient struct {
	*MongoClient
	options  mongo_driver_bson.DriverDoc //查询参数
	cursorId int64                       //对应的服务器游标
}

//做个包装 -- db, collection, filter (此3项必填)
func (cli *MongoClient) Find(db string, collection string, filter map[string]interface{}) *MongoFindClient {
	findCli := &MongoFindClient{
		MongoClient: cli,
	}
	findCli.options.Doc = make(bsonx.Doc, 0, 8)
	findCli.options.Append("find", bsonx.Doc(collection))
	findCli.options.Append("filter", )
	return findCli
}

//写链式调用 - sort (排序)
func (cli *MongoFindClient) Sort(sort interface{}) *MongoFindClient {
	cli.SortVal = sort
	return cli
}

//写链式调用 - select (选出field返回)
func (cli *MongoFindClient) Select(projection interface{}) *MongoFindClient {
	cli.Projection = projection
	return cli
}

//写链式调用 - skip（跳过cursor）
func (cli *MongoFindClient) Skip(skip int) *MongoFindClient {
	cli.SkipVal = skip
	return cli
}

//写链式调用-limit （限定返回值）
func (cli *MongoFindClient) Limit(limit int) *MongoFindClient {
	cli.LimitVal = limit
	return cli
}

//写链式调用-maxTimeMS （如果服务在指定的毫秒数内没有查询到相关的数据，则中断查询）
func (cli *MongoFindClient) MaxTimeMS(maxTimeMS int) *MongoFindClient {
	cli.MaxTimeMSVal = maxTimeMS
	return cli
}

//设置查询参数
func (cli *MongoFindClient) getFindMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	if cli.MaxTimeMSVal != 0 {
		findMsg.SetBodyDoc(*cli.FindMetaWithTimeout)
	} else {
		findMsg.SetBodyDoc(cli.FindBasic)
	}
	return findMsg
}

//设置分批查询参数
func (cli *MongoFindClient) getMoreMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	if cli.FindMetaWithTimeout.MaxTimeMSVal != 0 {
		findMsg.SetBodyDoc(wire_protocol.GetMoreWithTimeout{
			CursorId:       cli.cursorId,
			CollectionName: cli.CollectionName,
			MaxTimeMS:      cli.MaxTimeMSVal,
		})
	} else {
		findMsg.SetBodyDoc(wire_protocol.GetMore{
			CursorId:       cli.cursorId,
			CollectionName: cli.CollectionName,
		})
	}
	return findMsg
}

//设置关闭cursor参数
func (cli *MongoFindClient) getCloseCursorMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(wire_protocol.CursorKill{
		Db:             cli.Db,
		CollectionName: cli.CollectionName,
		CursorList:     []int64{cli.cursorId},
	})
	return findMsg
}

//检查命令参数
func (cli *MongoFindClient) verifyParam() error {
	if cli.Db == "" || cli.CollectionName == "" || cli.Filter == nil {
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

	cli.FindMetaWithTimeout.SingleBatch = true
	cli.LimitVal = 1
	inMsg := cli.getFindMsg()

	unmarshalfunc := func(buf []byte) error {
		return bson.UnMarshalBson(buf, val)
	}
	err = cli.MongoClient.RunAPIMsgWithBodyBinaryHandler(ctx, inMsg, unmarshalfunc)
	return
}
