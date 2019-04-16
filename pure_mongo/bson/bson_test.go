package bson

import (
	"bytes"
	driver_bson "github.com/globalsign/mgo/bson"
	mgo_bson "go.mongodb.org/mongo-driver/bson"
	"testing"
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
	Str string `bson:"str"`
}

func TestEncode5(t *testing.T) {
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
