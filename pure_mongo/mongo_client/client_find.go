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

	ErrFindDataErr     = errors.New("查询数据后服务端返回数据格式错误")
	ErrFindMoreDataErr = errors.New("获取更多查询数据后服务端返回数据格式错误")

	ErrKillCursorCmdInvalid = errors.New("注销cursor后服务端返回数据格式错误")
)

type MongoFindClient struct {
	*MongoClient
	options *wire_protocol.FindOption //查询参数
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

	cursor = newCursor(cli, findResult)
	return
}

var (
	ErrCursorInvalidIndex = errors.New("cursor的index超出范围")
)

//遍历
type Cursor struct {
	cursorId    int64                     //对应的服务器游标
	findResult  *wire_protocol.FindResult //当前查询结果
	cli         *MongoFindClient          //对应的client
	cursorIndex int                       //服务器游标
}

//新建cursor
func newCursor(cli *MongoFindClient, findResult *wire_protocol.FindResult) *Cursor {
	cursor := &Cursor{}
	cursor.resetCursor(cli, findResult)
	return cursor
}

//重置cursor
func (c *Cursor) resetCursor(cli *MongoFindClient, findResult *wire_protocol.FindResult) {
	c.cursorId = findResult.CursorId
	c.findResult = findResult
	c.cli = cli
	c.cursorIndex = -1
}

//设置分批查询参数
func (c *Cursor) getMoreMsg() *wire_protocol.APIMsg {
	getMoreMsg := wire_protocol.NewAPIMsg()
	if c.cli.options.MaxTimeMSVal != 0 {
		getMoreMsg.SetBodyDoc(wire_protocol.GetMoreWithTimeout{
			CursorId:       c.cursorId,
			CollectionName: c.cli.options.CollectionName,
			MaxTimeMS:      c.cli.options.MaxTimeMSVal,
		})
	} else {
		getMoreMsg.SetBodyDoc(wire_protocol.GetMore{
			CursorId:       c.cursorId,
			CollectionName: c.cli.options.CollectionName,
		})
	}
	return getMoreMsg
}

//设置关闭cursor参数
func (c *Cursor) getCloseCursorMsg() *wire_protocol.APIMsg {
	killMsg := wire_protocol.NewAPIMsg()
	killMsg.SetBodyDoc(wire_protocol.CursorKillReq{
		Db:             c.cli.options.Db,
		CollectionName: c.cli.options.CollectionName,
		CursorList:     []int64{c.cursorId},
	})
	return killMsg
}

//多个文档查询---一次返回不完，后续获取更多消息
func (c *Cursor) more(ctx context.Context) (err error) {
	var findResult *wire_protocol.FindResult

	if c.cursorId == 0 {
		//cursor为0则服务器已经无此cursor相关信息了
		return
	}
	inMsg := c.getMoreMsg()
	findResult, err = c.cli.MongoClient.RunFetchCmd(ctx, inMsg, "nextBatch")
	if err != nil {
		return
	}

	if findResult.OK == 0 {
		err = ErrFindMoreDataErr
		return
	}
	c.resetCursor(c.cli, findResult)
	return
}

//关闭游标
func (c *Cursor) Close(ctx context.Context) (
	killCursorResult *wire_protocol.CursorKillResult, err error) {
	if c.cursorId == 0 {
		//cursor为0则服务器已经无此cursor相关信息了
		return
	}
	inMsg := c.getCloseCursorMsg()
	killCursorResult = &wire_protocol.CursorKillResult{}
	err = c.cli.MongoClient.runAPIMsg(ctx, inMsg, killCursorResult, nil)
	if err != nil {
		return
	}

	if killCursorResult.OK == 0 {
		err = ErrKillCursorCmdInvalid
		return
	}
	//重置cursorId
	c.cursorId = 0
	return
}

//Count
func (c *Cursor) Count() int {
	return len(c.findResult.DocList)
}

//Next
func (c *Cursor) Next(ctx context.Context) (ok bool, err error) {
	docListLen := len(c.findResult.DocList)
	if docListLen == 0 {
		//没有任何数据
		return false, nil
	}

	c.cursorIndex++
	if c.cursorIndex >= len(c.findResult.DocList) {
		//此回合的遍历已经结束，需要去下个回合获取
		if c.cursorId == 0 {
			//没有数据了
			return false, nil
		}
		//开始新回合的数据获取
		err = c.more(ctx)
		if err != nil {
			return
		}

		//cursor现在已经重置
		return c.Next(ctx)
	}
	return true, nil
}

//Decode
func (c *Cursor) Decode(val interface{}) (err error) {
	docLen := len(c.findResult.DocList)
	if c.cursorIndex < 0 || c.cursorIndex >= docLen {
		return ErrCursorInvalidIndex
	}
	return bson.UnMarshalBson(c.findResult.DocList[c.cursorIndex], val)
}
