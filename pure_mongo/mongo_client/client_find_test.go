package connection

import (
	"testing"
	"time"
)

func TestMongoClient_FindOne1(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x map[string]interface{}
	err = cli.Find("a", "b", map[string]interface{}{}).One(ctx, &x)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		t.Logf("result is %+v", x)
	}
}

func TestMongoClient_FindOne2(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x XField
	err = cli.Find("a", "b", map[string]interface{}{"_id": 100000}).One(ctx, &x)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		t.Logf("result is %+v", x)
	}
}

func TestMongoClient_Find1(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x map[string]interface{}
	err = cli.Find("a", "b", map[string]interface{}{}).Find(ctx, &x)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		t.Logf("result is %+v", x)
	}
}
