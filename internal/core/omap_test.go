package core

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

// Goldens generated once with bun (JS JSON.stringify), the interop target:
// see evidence/node1-js-goldens.json.
const goldenCompact = `{"b":1,"a":{"x":[1,2],"y":{},"z":[]},"k":"한글<>&","n":1.5,"m":null,"t":true}`

const goldenIndent2 = `{
  "b": 1,
  "a": {
    "x": [
      1,
      2
    ],
    "y": {},
    "z": []
  },
  "k": "한글<>&",
  "n": 1.5,
  "m": null,
  "t": true
}`

func goldenOMap() *OMap {
	inner := NewOMap()
	inner.Set("x", []any{json.Number("1"), json.Number("2")})
	inner.Set("y", NewOMap())
	inner.Set("z", []any{})
	m := NewOMap()
	m.Set("b", json.Number("1"))
	m.Set("a", inner)
	m.Set("k", "한글<>&")
	m.Set("n", 1.5)
	m.Set("m", nil)
	m.Set("t", true)
	return m
}

func TestMarshalCompactGolden(t *testing.T) {
	b, err := goldenOMap().Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != goldenCompact {
		t.Errorf("compact = %q\nwant        %q", b, goldenCompact)
	}
}

func TestMarshalIndent2Golden(t *testing.T) {
	b, err := goldenOMap().MarshalIndent2()
	if err != nil {
		t.Fatalf("marshal indent2: %v", err)
	}
	if string(b) != goldenIndent2 {
		t.Errorf("indent2 = %q\nwant %q", b, goldenIndent2)
	}
}

func TestParseJSONRoundTripOrder(t *testing.T) {
	docs := []string{
		goldenCompact,
		`{"z":1,"a":{"y":[true,null,"s"],"x":{}},"m":[{"k":2},{}],"arr":[]}`,
	}
	for _, doc := range docs {
		v, err := ParseJSON([]byte(doc))
		if err != nil {
			t.Fatalf("parse %q: %v", doc, err)
		}
		b, err := v.(*OMap).Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(b) != doc {
			t.Errorf("round-trip:\n got %s\nwant %s", b, doc)
		}
	}
}

func TestOMapSetKeepsPositionAndAppends(t *testing.T) {
	m, err := ParseJSON([]byte(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	om := m.(*OMap)
	om.Set("c", json.Number("3"))
	om.Set("a", json.Number("9"))
	b, _ := om.Marshal()
	if string(b) != `{"a":9,"b":2,"c":3}` {
		t.Errorf("set: got %s", b)
	}
	if !reflect.DeepEqual(om.Keys(), []string{"a", "b", "c"}) {
		t.Errorf("keys = %v", om.Keys())
	}
}

func TestOMapGetHasDeleteLen(t *testing.T) {
	m, err := ParseJSON([]byte(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	om := m.(*OMap)
	if om.Len() != 2 {
		t.Errorf("len = %d", om.Len())
	}
	if v, ok := om.Get("a"); !ok || v.(json.Number).String() != "1" {
		t.Errorf("get a = %v %v", v, ok)
	}
	if _, ok := om.Get("zz"); ok {
		t.Error("get zz should miss")
	}
	if !om.Has("b") || om.Has("zz") {
		t.Errorf("has: b=%v zz=%v", om.Has("b"), om.Has("zz"))
	}
	om.Delete("a")
	b, _ := om.Marshal()
	if string(b) != `{"b":2}` {
		t.Errorf("after delete: %s", b)
	}
	om.Delete("missing") // no-op
	if om.Len() != 1 {
		t.Errorf("len after no-op delete = %d", om.Len())
	}
}

func TestParseJSONDuplicateKeysLastWins(t *testing.T) {
	v, err := ParseJSON([]byte(`{"a":1,"b":0,"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := v.(*OMap).Marshal()
	// JS JSON.parse: later value wins, position of first occurrence kept.
	if string(b) != `{"a":2,"b":0}` {
		t.Errorf("dup keys: got %s", b)
	}
}

func TestParseJSONScalarsAndEmpties(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{"null", nil},
		{"true", true},
		{"1.5", json.Number("1.5")},
		{`"한글"`, "한글"},
	}
	for _, c := range cases {
		v, err := ParseJSON([]byte(c.in))
		if err != nil {
			t.Fatalf("parse %s: %v", c.in, err)
		}
		if !reflect.DeepEqual(v, c.want) {
			t.Errorf("parse %s = %#v, want %#v", c.in, v, c.want)
		}
	}
	v, _ := ParseJSON([]byte("{}"))
	if m := v.(*OMap); m.Len() != 0 {
		t.Errorf("empty object len = %d", m.Len())
	}
	v, _ = ParseJSON([]byte("[]"))
	arr := v.([]any)
	if len(arr) != 0 {
		t.Errorf("empty array len = %d", len(arr))
	}
	b, _ := NewOMap().Marshal()
	if string(b) != "{}" {
		t.Errorf("empty marshal = %s", b)
	}
	b, _ = NewOMap().MarshalIndent2()
	if string(b) != "{}" {
		t.Errorf("empty indent2 = %s", b)
	}
}

func TestParseJSONBigNumberKeepsForm(t *testing.T) {
	// json.Number passthrough: big integers keep their literal form.
	v, err := ParseJSON([]byte(`{"n":12345678901234567890123}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := v.(*OMap).Marshal()
	if string(b) != `{"n":12345678901234567890123}` {
		t.Errorf("big number: got %s", b)
	}
}

func TestParseJSONErrors(t *testing.T) {
	for _, in := range []string{"", "{} x", "{", `{"a"`, "[1,]", "nope"} {
		if _, err := ParseJSON([]byte(in)); err == nil {
			t.Errorf("parse %q: expected error", in)
		}
	}
}

func TestUnmarshalJSON(t *testing.T) {
	om := NewOMap()
	if err := om.UnmarshalJSON([]byte(`{"b":1,"a":2}`)); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(om.Keys(), []string{"b", "a"}) {
		t.Errorf("keys = %v", om.Keys())
	}
	if err := NewOMap().UnmarshalJSON([]byte(`[1]`)); err == nil {
		t.Error("non-object document must error")
	}
}

func TestMarshalNumbers(t *testing.T) {
	cases := []struct {
		f    float64
		want string
	}{
		{1.5, "1.5"},
		{1e21, "1e+21"},
		{1e-7, "1e-7"},
		{1e-6, "0.000001"},
		{0, "0"},
		{123.456, "123.456"},
		{1.2345678901234568e20, "123456789012345680000"},
		{5e-324, "5e-324"},
		{-1.5, "-1.5"},
	}
	for _, c := range cases {
		m := NewOMap()
		m.Set("n", c.f)
		b, err := m.Marshal()
		if err != nil {
			t.Fatalf("marshal %v: %v", c.f, err)
		}
		if string(b) != `{"n":`+c.want+`}` {
			t.Errorf("number %v: got %s, want %s", c.f, b, c.want)
		}
	}
	m := NewOMap()
	m.Set("inf", math.Inf(1))
	m.Set("nan", math.NaN())
	m.Set("i", 7)
	b, _ := m.Marshal()
	if string(b) != `{"inf":null,"nan":null,"i":7}` {
		t.Errorf("non-finite/int: got %s", b)
	}
}

func TestMarshalControlChars(t *testing.T) {
	// bun JSON.stringify uses \b and \f shortcuts.
	m := NewOMap()
	m.Set("c", "\b\f\n")
	b, _ := m.Marshal()
	if string(b) != `{"c":"`+`\b\f\n`+`"}` {
		t.Errorf("control chars: got %q", b)
	}
}
