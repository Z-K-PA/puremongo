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
	options  *wire_protocol.FindOption //查询参数
	cursorId int64                     //对应的服务器游标
}

//做个包装 -- db, collection, filter (此3项必填)
func (cli *MongoClient) Find(db string, collection string, filter map[string]interface{}) *MongoFindClient {
	findCli := &MongoFindClient{
		MongoClient: cli,
		options:     &wire_protocol.FindOption{},
	}
	findCli.options.Db = db
	findCli.options.CollectionName = collection
	findCli.options.Filter = filter
	return findCli
}

//发送find相关后接收消息
func (cli *MongoClient) RunFetchCmd(
	ctx context.Context,
	inMsg *wire_protocol.APIMsg,
	batchKey string) (findResult *wire_protocol.FindResult, err error) {

	var outMsg *wire_protocol.APIMsg

	outMsg, err = cli.sendAPIMsgRecvAPIMsg(ctx, inMsg)
	if err != nil {
		return
	}
	findResult = &wire_protocol.FindResult{}

	err = findResult.FromBuffer(outMsg.Body.Doc.Buf, "firstBatch")
	return
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
	var findResult *wire_protocol.FindResult
	err = cli.verifyParam()
	if err != nil {
		return
	}

	cli.options.SingleBatch = true
	cli.options.LimitVal = 1
	inMsg := cli.getFindMsg()

	findResult, err = cli.MongoClient.RunFetchCmd(ctx, inMsg, "firstBatch")
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

//多个文档的查询
func (cli *MongoFindClient) Iter(ctx context.Context) (cursor *Cursor, err error) {
	var findResult *wire_protocol.FindResult
	err = cli.verifyParam()
	if err != nil {
		return
	}

	inMsg := cli.getFindMsg()

	findResult, err = cli.MongoClient.RunFetchCmd(ctx, inMsg, "firstBatch")
	if err != nil {
		return
	}

	if findResult.OK == 0 {
		err = ErrFindDataErr
		return
	}

	cursor = newCursor(findResult.DocList)
	return
}

var (
	ErrCursorInvalidIndex = errors.New("cursor的index超出范围")
)

//遍历
type Cursor struct {
	docList     bson.ArrayDoc
	cursorIndex int
}

//新建cursor
func newCursor(docList bson.ArrayDoc) *Cursor {
	cursor := &Cursor{
		docList:     docList,
		cursorIndex: -1,
	}
	return cursor
}

//Count
func (c *Cursor) Count() int {
	return len(c.docList)
}

//Next
func (c *Cursor) Next() bool {
	c.cursorIndex++
	if c.cursorIndex >= len(c.docList) {
		return false
	}
	return true
}

//Decode
func (c *Cursor) Decode(val interface{}) (err error) {
	docLen := len(c.docList)
	if c.cursorIndex < 0 || c.cursorIndex >= docLen {
		return ErrCursorInvalidIndex
	}
	return bson.UnMarshalBson(c.docList[c.cursorIndex], val)
}
