package connection

import (
	"context"
	"errors"
	"pure_mongos/pure_mongo/bson"
	"pure_mongos/pure_mongo/wire_protocol"
)

var (
	ErrCursorInvalidIndex = errors.New("cursor的index超出范围")
)

type IFetchMongoClient interface {
	//获取实际的client
	GetMongoClient() *MongoClient
	//根据查询参数生成查询消息
	GetFindMsg() *wire_protocol.APIMsg
	//校验参数是否合法
	VerifyParam() error
	//设定只查询一条数据的参数
	SetOnce()
}

//遍历
type Cursor struct {
	db          string                    //db name
	collection  string                    //collection name
	maxTimeMs   int32                     //在服务器查询的最长时间，超过后服务器会终止查询
	cursorId    int64                     //对应的服务器游标
	findResult  *wire_protocol.FindResult //当前查询结果
	cli         IFetchMongoClient         //对应的client
	cursorIndex int                       //服务器游标
}

//新建cursor
func newCursor(
	cli IFetchMongoClient,
	findResult *wire_protocol.FindResult,
	db string,
	collection string,
	maxTimeMs int32) *Cursor {

	cursor := &Cursor{
		db:         db,
		collection: collection,
		maxTimeMs:  maxTimeMs,
		cli:        cli,
	}
	cursor.resetCursor(findResult)
	return cursor
}

//重置cursor
func (c *Cursor) resetCursor(findResult *wire_protocol.FindResult) {
	c.cursorId = findResult.CursorId
	c.findResult = findResult
	c.cursorIndex = -1
}

//设置分批查询参数
func (c *Cursor) getMoreMsg() *wire_protocol.APIMsg {
	getMoreMsg := wire_protocol.NewAPIMsg()

	if c.maxTimeMs != 0 {
		getMoreMsg.SetBodyDoc(wire_protocol.GetMoreWithTimeout{
			CursorId:       c.cursorId,
			Db:             c.db,
			CollectionName: c.collection,
			MaxTimeMS:      c.maxTimeMs,
		})
	} else {
		getMoreMsg.SetBodyDoc(wire_protocol.GetMore{
			CursorId:       c.cursorId,
			Db:             c.db,
			CollectionName: c.collection,
		})
	}
	return getMoreMsg
}

//设置关闭cursor参数
func (c *Cursor) getCloseCursorMsg() *wire_protocol.APIMsg {
	killMsg := wire_protocol.NewAPIMsg()
	killMsg.SetBodyDoc(wire_protocol.CursorKillReq{
		CollectionName: c.collection,
		Db:             c.db,
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
	findResult, err = c.cli.GetMongoClient().runFetchCmd(ctx, inMsg, "nextBatch")
	if err != nil {
		return
	}

	if findResult.OK == 0 {
		err = ErrFindMoreDataErr
		return
	}
	c.resetCursor(findResult)
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
	err = c.cli.GetMongoClient().runAPIMsg(ctx, inMsg, killCursorResult, nil)
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
		//此回合没有任何数据，没必要进行下一个回合了
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
