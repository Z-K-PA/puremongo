package connection

import (
	"testing"
	"time"
)

func TestMongoClient_FindOne(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	if err != nil {
		t.Errorf("init error is %+v", err)
	}

	var x map[string]interface{}
	err = cli.Find("a", "b", map[string]interface{}{}).One(ctx, &x)
	if err != nil {
		t.Errorf("init error is %+v", err)
	} else {
		t.Logf("result is %+v", x)
	}
}
