package connection

import (
	"context"
	"errors"
	"fmt"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	//count使用的aggregate pipeline
	_countAggregatePipeline = []interface{}{
		bson.Hash{
			"$match": bson.Hash{},
		},
		bson.Hash{
			"$group": bson.Hash{
				"_id": nil,
				"n": bson.Hash{
					"$sum": 1,
				},
			},
		},
	}
)

var (
	ErrCountCmdInvalid = errors.New("查询数据数量命令不正确")
	ErrCountDataErr    = errors.New("查询数据数量后服务端返回数据格式错误")
)

//用aggregate
func (cli *MongoClient) CountV2(
	ctx context.Context,
	db string,
	collectionName string,
	filter interface{}) (count int, err error) {
	var countN wire_protocol.CountN

	err = cli.Aggregate(db, collectionName).Pipeline(_countAggregatePipeline...).One(ctx, &countN)

	if err != nil {
		return
	}

	count = countN.N
	return
}

type MongoCountClient struct {
	*MongoClient
	options *wire_protocol.CountOption
}

//做个包装 -- db, collection, query(此3项必填)
func (cli *MongoClient) Count(db string, collection string, query interface{}) *MongoCountClient {
	findCli := &MongoCountClient{
		MongoClient: cli,
		options:     &wire_protocol.CountOption{},
	}
	findCli.options.Db = db
	findCli.options.CollectionName = collection
	findCli.options.Query = query
	return findCli
}

//写链式调用 - 设置skip
func (cli *MongoCountClient) Skip(skip int32) *MongoCountClient {
	cli.options.Skip = skip
	return cli
}

//写链式调用 - 设置limit
func (cli *MongoCountClient) Limit(limit int32) *MongoCountClient {
	cli.options.Limit = limit
	return cli
}

//获取查询消息
func (cli *MongoCountClient) GetCountMsg() *wire_protocol.APIMsg {
	findMsg := wire_protocol.NewAPIMsg()
	findMsg.SetBodyDoc(cli.options)
	return findMsg
}

//检查命令参数
func (cli *MongoCountClient) VerifyParam() error {
	if cli.options.Db == "" || cli.options.CollectionName == "" {
		return ErrCountCmdInvalid
	} else {
		if cli.options.Query != nil {
			cli.options.Query = bson.Hash{}
		}
		return nil
	}
}

//查询结果
func (cli *MongoCountClient) Get(ctx context.Context) (count int, err error) {
	err = cli.VerifyParam()
	if err != nil {
		return
	}

	inMsg := cli.GetCountMsg()
	countResult := &wire_protocol.CountResult{}

	err = cli.MongoClient.runAPIMsg(ctx, inMsg, countResult, nil)
	if err != nil {
		return
	}

	if countResult.OK == 0 {
		err = ErrCountDataErr
		fmt.Printf("count result is %+v", countResult)
		return
	}
	count = countResult.N
	return
}
