package legacy

/*
func testPrepare2(t *testing.T, duration time.Duration) (*MongoClient, context.Context, context.CancelFunc, error) {
	mgo_bson.InitDriver()

	ctx, cancel := context.WithTimeout(context.Background(), duration)

	cli, err := DialMongoClient(ctx, &net.Dialer{
		KeepAlive: 3 * time.Minute,
	}, "localhost:27017")
	if err != nil {
		t.Errorf("connect error :%+v", err)
	}

	return cli, ctx, cancel, err
}

func TestBaseMongoClient_Master1(t *testing.T) {
	var hashList []map[string]interface{}
	var item map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	qMsg := wire_protocol.NewQueryMsg()
	qMsg.AddDoc(bson.DocPair{Name: "ismaster", Value: bsonx.Int32(1)})

	err = cli.queryWithHandler(ctx, qMsg, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

func TestBaseMongoClient_Master2(t *testing.T) {
	var hashList []map[string]interface{}
	var item map[string]interface{}

	cli, ctx, cancel, err := testPrepare1(t, 3*time.Second)
	defer cancel()

	handler := func() {
		hashList = append(hashList, item)
	}

	wire_protocol.InitIsMasterBuffer()
	err = cli.queryBufWithHandler(ctx, wire_protocol.IsMasterMsgBuf, handler, &item)
	if err != nil {
		t.Errorf("query error :%+v", err)
	} else {
		t.Logf("hash list is %+v", hashList)
	}
}

*/
