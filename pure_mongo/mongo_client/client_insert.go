package connection

import (
	"context"
	"errors"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrInsertDataErr = errors.New("插入数据后服务端返回数据格式错误")
)

//插入一条记录
func (cli *MongoClient) InsertOne(
	ctx context.Context,
	db string,
	collection string,
	val interface{}) (
	insertResult *wire_protocol.InsertResult,
	err error) {

	insertResult = &wire_protocol.InsertResult{}
	inMsg := wire_protocol.NewInsertOneMessage(db, collection, true, val)
	err = cli.runAPIMsg(ctx, inMsg, insertResult, nil)
	if err != nil {
		return
	}

	if insertResult.OK == 0 {
		err = ErrInsertDataErr
		return
	}

	return
}

//插入多条记录 -- 参数为切片
func (cli *MongoClient) InsertMany(
	ctx context.Context,
	db string,
	collection string,
	val interface{},
	ordered bool) (
	insertResult *wire_protocol.InsertResult,
	err error) {

	var inMsg *wire_protocol.APIMsg
	insertResult = &wire_protocol.InsertResult{}
	inMsg, err = wire_protocol.NewInsertManyMessage(db, collection, ordered, val)
	if err != nil {
		return
	}

	err = cli.runAPIMsg(ctx, inMsg, insertResult, nil)
	if err != nil {
		return
	}

	if insertResult.OK == 0 {
		err = ErrInsertDataErr
		return
	}

	return
}

//插入多条记录 -- 参数为interface切片
func (cli *MongoClient) InsertManyI(
	ctx context.Context,
	db string,
	collection string,
	val []interface{},
	ordered bool) (
	insertResult *wire_protocol.InsertResult,
	err error) {

	var inMsg *wire_protocol.APIMsg
	insertResult = &wire_protocol.InsertResult{}
	inMsg = wire_protocol.NewInsertManyMessageI(db, collection, ordered, val)

	err = cli.runAPIMsg(ctx, inMsg, insertResult, nil)
	if err != nil {
		return
	}

	if insertResult.OK == 0 {
		err = ErrInsertDataErr
		return
	}

	return
}
