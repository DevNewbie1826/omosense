package google

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "zele", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

// normalize turns core values into comparable shapes: numbers (json.Number,
// int, int64, float64) collapse to float64 and OMaps keep their key order.
func normalize(v any) any {
	switch x := v.(type) {
	case *core.OMap:
		out := []any{}
		for _, k := range x.Keys() {
			vv, _ := x.Get(k)
			out = append(out, []any{k, normalize(vv)})
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalize(e)
		}
		return out
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return x.String()
		}
		return f
	case int:
		return float64(x)
	case int64:
		return float64(x)
	default:
		return v
	}
}

// TestYAMLScalarDifferential pins parseYAML to the committed Bun golden
// dump (Bun.YAML.parse + JSON.stringify, generated once with bun): every
// scalar of every fixture must resolve identically, including quoting
// styles, the 0x/0o signed forms and the 2^53 float semantics.
func TestYAMLScalarDifferential(t *testing.T) {
	gv, err := core.ParseJSON(fixture(t, "yaml.golden.json"))
	if err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	golden := gv.(*core.OMap)
	for _, f := range []string{"cal.yaml", "mail.yaml", "scalars.yaml"} {
		got, err := parseYAML(fixture(t, f))
		if err != nil {
			t.Fatalf("%s: parse: %v", f, err)
		}
		want, _ := golden.Get(f)
		if !reflect.DeepEqual(normalize(got), normalize(want)) {
			t.Errorf("%s: parsed value differs from Bun golden:\n got: %#v\nwant: %#v", f, normalize(got), normalize(want))
		}
	}
}

// TestFixturesParseInOrder pins the masked fixtures' structure: items in
// file order with zele's exact key order, the U+202F timed form intact and
// numeric types resolved as numbers.
func TestFixturesParseInOrder(t *testing.T) {
	cv, err := parseYAML(fixture(t, "cal.yaml"))
	if err != nil {
		t.Fatalf("cal: %v", err)
	}
	cm := cv.(*core.OMap)
	iv, _ := cm.Get("items")
	items := iv.([]any)
	if len(items) != 5 {
		t.Fatalf("cal items = %d, want 5", len(items))
	}
	first := items[0].(*core.OMap)
	if got := first.Keys(); !reflect.DeepEqual(got, []string{"account", "id", "summary", "start", "end", "calendar"}) {
		t.Errorf("cal item keys = %v", got)
	}
	starts := []any{}
	for _, it := range items {
		sv, _ := it.(*core.OMap).Get("start")
		starts = append(starts, sv)
	}
	if !reflect.DeepEqual(starts, []any{"Oct 5", "Oct 5", "Oct 6, 9:30 AM", "Oct 7, 10:00\u202fPM", "Oct 8, 21:30"}) {
		t.Errorf("cal starts = %#v", starts)
	}

	mv, err := parseYAML(fixture(t, "mail.yaml"))
	if err != nil {
		t.Fatalf("mail: %v", err)
	}
	mm := mv.(*core.OMap)
	iv, _ = mm.Get("items")
	mitems := iv.([]any)
	if len(mitems) != 5 {
		t.Fatalf("mail items = %d, want 5", len(mitems))
	}
	m0 := mitems[0].(*core.OMap)
	if got := m0.Keys(); !reflect.DeepEqual(got, []string{"account", "id", "flags", "from", "to", "subject", "snippet", "date", "messages", "labels"}) {
		t.Errorf("mail item 0 keys = %v", got)
	}
	m1 := mitems[1].(*core.OMap)
	if got := m1.Keys(); !reflect.DeepEqual(got, []string{"account", "id", "flags", "from", "to", "subject", "snippet", "date", "messages", "labels"}) {
		t.Errorf("mail item 1 keys = %v", got)
	}
	nv, _ := m0.Get("messages")
	if f, ok := nv.(float64); !ok || f != 1 {
		t.Errorf("messages = %#v, want float64(1)", nv)
	}
}
