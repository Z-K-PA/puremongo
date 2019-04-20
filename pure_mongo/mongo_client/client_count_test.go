package connection

import (
	"pure_mongos/pure_mongo/bson"
	"testing"
	"time"
)

func TestMongoClient_CountV2(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	count, err := cli.CountV2(ctx, "a", "b", bson.Hash{})
	t.Logf("count is %+v error is %+v", count, err)
}

func TestMongoClient_Count(t *testing.T) {
	cli, ctx, cancel, err := testPrepare1(t, 300*time.Second)
	defer cancel()

	count, err := cli.Count("a", "b", bson.Hash{}).Get(ctx)

	t.Logf("count is %+v error is %+v", count, err)
}
