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
	cursor, err := cli.Find("a", "b", map[string]interface{}{}).Iter(ctx)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		defer cursor.Close(ctx)
		for {
			ok, err := cursor.Next(ctx)
			if err != nil {
				t.Errorf("cursor error:%+v", err)
				return
			}
			if !ok {
				t.Logf("cursor end")
				return
			}
			err = cursor.Decode(&x)
			if err != nil {
				t.Errorf("decode error:%+v", err)
				return
			}
			t.Logf("cursorid:%+v  --- %+v", cursor.cursorId, x)
		}
	}
}

func TestMongoClient_Find2(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x map[string]interface{}
	cursor, err := cli.Find("a", "b", map[string]interface{}{}).Iter(ctx)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		defer cursor.Close(ctx)
		i := 0
		for {
			if i >= 100 {
				break
			}
			ok, err := cursor.Next(ctx)
			if err != nil {
				t.Errorf("cursor error:%+v", err)
				return
			}
			if !ok {
				t.Logf("cursor end")
				return
			}
			err = cursor.Decode(&x)
			if err != nil {
				t.Errorf("decode error:%+v", err)
				return
			}
			t.Logf("cursorid:%+v  --- %+v", cursor.cursorId, x)
			i++
		}
	}
}

func TestMongoClient_Find3(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 15*time.Minute)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x map[string]interface{}
	cursor, err := cli.Find("a", "b", map[string]interface{}{}).Iter(ctx)
	if err != nil {
		t.Errorf("find error is %+v", err)
	} else {
		defer cursor.Close(ctx)
		i := 0
		for {
			if i >= 100 {
				break
			}
			ok, err := cursor.Next(ctx)
			if err != nil {
				t.Errorf("cursor error:%+v", err)
				return
			}
			if !ok {
				t.Logf("cursor end")
				return
			}
			err = cursor.Decode(&x)
			if err != nil {
				t.Errorf("decode error:%+v", err)
				return
			}
			t.Logf("cursorid:%+v  --- %+v", cursor.cursorId, x)
			i++
		}
		time.Sleep(11 * time.Minute)
	}
}
