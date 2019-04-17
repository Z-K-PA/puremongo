package bson

import (
	"bytes"
	driver_bson "github.com/globalsign/mgo/bson"
	mgo_bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"testing"
	"time"
)

type XField struct {
	IsMaster int `bson:"a"`
}

func TestEncode1(t *testing.T) {
	var x XField

	buf1, err := mgo_bson.Marshal(x)
	if err != nil {
		t.Errorf("1 marshal error:%+v\n", err)
	}
	buf2, err := driver_bson.Marshal(x)
	if err != nil {
		t.Errorf("2 marshal error:%+v\n", err)
	}
	if !bytes.Equal(buf1, buf2) {
		t.Fail()
	}
	t.Logf("%+v\n", buf1)
}

func TestEncode2(t *testing.T) {
	var x XField

	buf1, err := mgo_bson.Marshal(x)
	if err != nil {
		t.Errorf("1 marshal error:%+v\n", err)
	}

	m := map[string]interface{}{
		"a": 0,
	}
	buf2, err := driver_bson.Marshal(m)
	if err != nil {
		t.Errorf("2 marshal error:%+v\n", err)
	}
	if !bytes.Equal(buf1, buf2) {
		t.Fail()
	}
	t.Logf("%+v\n", buf1)
}

func TestEncode3(t *testing.T) {
	var x []XField

	buf1, err := mgo_bson.Marshal(XField{})
	if err != nil {
		t.Errorf("1 marshal error:%+v\n", err)
	}
	t.Logf("%+v\n", buf1)

	x = make([]XField, 2)
	buf2, err := mgo_bson.Marshal(x)
	if err != nil {
		t.Errorf("2 marshal error:%+v\n", err)
	}
	t.Logf("%+v\n", buf2)
}

type XSField struct {
	Str string `bson:"a"`
}

func TestEncodeS(t *testing.T) {
	v := XSField{
		Str: "a",
	}
	buf, err := driver_bson.Marshal(v)

	t.Logf("err of buf:%+v", err)
	t.Logf("buf:%+v", buf)
}

func TestEncode4(t *testing.T) {
	x := make([]byte, 1024*1024)
	for i := range x {
		x[i] = 'a'
	}
	v := XSField{
		Str: string(x),
	}
	buf, err := driver_bson.Marshal(v)

	t.Logf("err of buf:%+v", err)
	t.Logf("len of buf:%+v", len(buf))
	t.Logf("end of buf:%+v", buf[len(buf)-8:])
}

func TestEncode5(t *testing.T) {
	var x bsonx.Doc

	x = x.Append("a", bsonx.Int32(1))
	x = x.Append("b", bsonx.String("abc"))

	buf1, err := driver_bson.Marshal(x)
	if err != nil {
		t.Fail()
	}
	t.Logf("buf1 is %+v", buf1)

	buf2, err := x.MarshalBSON()
	if err != nil {
		t.Fail()
	}
	t.Logf("buf2 is %+v", buf2)

	var x1, x2 map[string]interface{}

	err = driver_bson.Unmarshal(buf1, &x1)
	if err != nil {
		t.Fail()
	}

	err = driver_bson.Unmarshal(buf2, &x2)
	if err != nil {
		t.Fail()
	}

	t.Logf("x1 is %+v", x1)
	t.Logf("x2 is %+v", x2)
}

func TestInterface(t *testing.T) {
	var x bsonx.Doc

	x = x.Append("a", bsonx.Int32(1))
	x = x.Append("b", bsonx.String("abc"))

	y := XSField{
		Str: "c",
	}

	l := 10000000

	bList := make([]interface{}, l)

	for i := 0; i < l; i++ {
		if i%2 == 0 {
			bList[i] = x
		} else {
			bList[i] = y
		}
	}

	t1 := time.Now()
	for i := 0; i < l; i++ {
		switch bList[i].(type) {
		case bsonx.Doc:
			x = bList[i].(bsonx.Doc)
			bList[i] = x
		}
	}
	t2 := time.Now()
	t.Logf("total time :%+v -- average time:%+v", t2.Sub(t1), t2.Sub(t1)/time.Duration(l))
}
