package mgo_bson

import (
	"pure_mongos/pure_mongo/bson"
	"testing"
	"time"
)

func getTestMap() map[string]interface{} {
	x := make(map[string]interface{})
	x["field1"] = 1
	x["field2"] = `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`
	x["field3"] = `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`
	x["field4"] = `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`
	x["field5"] = 90000000000
	return x
}

type XField struct {
	Field1 int64  `bson:"field1"`
	Field2 string `bson:"field2"`
	Field3 string `bson:"field3"`
	Field4 string `bson:"field4"`
	Field5 int64  `bson:"field5"`
}

func getTestStruct() XField {
	return XField{
		Field1: 1,
		Field2: `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`,
		Field3: `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`,
		Field4: `abcdefgg sffdadfa lll1l3   424243432 234232ll  afadafal llllnnkcnooqn lozooo1n130445454lll sssss ll`,
		Field5: 90000000000,
	}
}

func TestMgoBufferWithMap(t *testing.T) {
	InitDriver()
	x := getTestMap()
	cacheBuf := make([]byte, 16*1024)

	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		var xVal map[string]interface{}
		outL, err := bson.MarshalBsonWithBuffer(x, &cacheBuf, 0)
		if int(outL) > len(cacheBuf) {
			t.Fail()
		}
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
		err = bson.UnMarshalBson(cacheBuf[:outL], &xVal)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
	}
	t2 := time.Now()
	t.Logf("marshal time:%+v", t2.Sub(t1)/100000)
}

func TestMgoWithMap(t *testing.T) {
	InitDriver()
	x := getTestMap()
	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		var xVal map[string]interface{}
		outBuf, err := bson.MarshalBson(x)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
		err = bson.UnMarshalBson(outBuf, &xVal)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
	}
	t2 := time.Now()
	t.Logf("marshal time:%+v", t2.Sub(t1)/100000)
}

func TestMgoBufferWithStruct(t *testing.T) {
	InitDriver()
	x := getTestStruct()
	cacheBuf := make([]byte, 16*1024)

	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		var xVal XField
		outL, err := bson.MarshalBsonWithBuffer(x, &cacheBuf, 0)
		if int(outL) > len(cacheBuf) {
			t.Fail()
		}
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
		err = bson.UnMarshalBson(cacheBuf[:outL], &xVal)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
	}
	t2 := time.Now()
	t.Logf("marshal time:%+v", t2.Sub(t1)/100000)
}

func TestMgoWithStruct(t *testing.T) {
	InitDriver()
	x := getTestStruct()
	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		var xVal XField
		outBuf, err := bson.MarshalBson(x)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
		err = bson.UnMarshalBson(outBuf, &xVal)
		if err != nil {
			t.Errorf("marsal error :%+v", err)
		}
	}
	t2 := time.Now()
	t.Logf("marshal time:%+v", t2.Sub(t1)/100000)
}
