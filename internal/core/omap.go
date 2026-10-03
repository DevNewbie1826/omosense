package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// OMap is a JSON object that preserves key insertion order. Values are nil,
// bool, string, json.Number, int, int64, float64, []any, or *OMap. Marshal
// and MarshalIndent2 reproduce bun's JSON.stringify byte layout, and
// ParseJSON keeps document order (numbers become json.Number). A nil *OMap
// is safe to read from; mutating methods on it are no-ops. OMap is not
// safe for concurrent use.
type OMap struct {
	keys []string
	vals map[string]any
}

// NewOMap returns an empty OMap.
func NewOMap() *OMap { return &OMap{vals: map[string]any{}} }

// Len returns the number of keys.
func (m *OMap) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

// Keys returns the keys in insertion order.
func (m *OMap) Keys() []string {
	if m == nil {
		return nil
	}
	out := make([]string, len(m.keys))
	copy(out, m.keys)
	return out
}

// Has reports whether key is present.
func (m *OMap) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

// Get returns the value of key.
func (m *OMap) Get(key string) (any, bool) {
	if m == nil {
		return nil, false
	}
	v, ok := m.vals[key]
	return v, ok
}

// Set stores the value: an existing key keeps its position, a new key is
// appended.
func (m *OMap) Set(key string, v any) {
	if m == nil {
		return
	}
	if m.vals == nil {
		m.vals = map[string]any{}
	}
	if _, ok := m.vals[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.vals[key] = v
}

// Delete removes key, keeping the order of the remaining keys.
func (m *OMap) Delete(key string) {
	if m == nil {
		return
	}
	if _, ok := m.vals[key]; !ok {
		return
	}
	delete(m.vals, key)
	for i, k := range m.keys {
		if k == key {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

// Marshal renders the object as compact JSON, byte-compatible with bun's
// JSON.stringify (no HTML escaping).
func (m *OMap) Marshal() ([]byte, error) {
	var buf []byte
	return appendValue(buf, m, "")
}

// MarshalIndent2 renders the object in the JSON.stringify(v, null, 2)
// layout: two-space indent, empty containers as {} and [], no HTML
// escaping.
func (m *OMap) MarshalIndent2() ([]byte, error) {
	var buf []byte
	return appendValue(buf, m, "  ")
}

// MarshalJSON implements json.Marshaler as compact output.
func (m *OMap) MarshalJSON() ([]byte, error) { return m.Marshal() }

// UnmarshalJSON parses a JSON object into m, preserving key order.
func (m *OMap) UnmarshalJSON(data []byte) error {
	v, err := ParseJSON(data)
	if err != nil {
		return err
	}
	om, ok := v.(*OMap)
	if !ok {
		return fmt.Errorf("omap: expected a JSON object")
	}
	m.keys = om.keys
	m.vals = om.vals
	return nil
}

// ParseJSON parses a JSON document preserving object key order: objects
// become *OMap, arrays []any, numbers json.Number. Trailing data is an
// error, matching JSON.parse.
func ParseJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return v, nil
}

func parseValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return tokenValue(dec, tok)
}

func tokenValue(dec *json.Decoder, tok json.Token) (any, error) {
	if d, ok := tok.(json.Delim); ok {
		switch d {
		case '{':
			m := NewOMap()
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, fmt.Errorf("bad object key %v", keyTok)
				}
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				m.Set(key, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return arr, nil
		default:
			return nil, fmt.Errorf("unexpected delimiter %v", d)
		}
	}
	return tok, nil
}

// appendValue renders v with child as the prefix of its child lines: ""
// produces compact output, "  " the JSON.stringify(v, null, 2) layout.
func appendValue(buf []byte, v any, child string) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		buf = append(buf, "null"...)
	case bool:
		buf = strconv.AppendBool(buf, x)
	case string:
		buf = appendString(buf, x)
	case json.Number:
		buf = append(buf, x.String()...)
	case int:
		buf = strconv.AppendInt(buf, int64(x), 10)
	case int64:
		buf = strconv.AppendInt(buf, x, 10)
	case float64:
		buf = append(buf, jsNumber(x)...)
	case *OMap:
		buf = append(buf, '{')
		for i, k := range x.keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = childLine(buf, child)
			buf = appendString(buf, k)
			buf = appendValueSep(buf, child)
			var err error
			if buf, err = appendValue(buf, x.vals[k], childIndent(child)); err != nil {
				return nil, err
			}
		}
		if len(x.keys) > 0 && child != "" {
			buf = closeLine(buf, child)
		}
		buf = append(buf, '}')
	case []any:
		buf = append(buf, '[')
		for i, e := range x {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = childLine(buf, child)
			var err error
			if buf, err = appendValue(buf, e, childIndent(child)); err != nil {
				return nil, err
			}
		}
		if len(x) > 0 && child != "" {
			buf = closeLine(buf, child)
		}
		buf = append(buf, ']')
	default:
		return nil, fmt.Errorf("omap: unsupported value type %T", v)
	}
	return buf, nil
}

func childLine(buf []byte, child string) []byte {
	if child == "" {
		return buf
	}
	return append(append(buf, '\n'), child...)
}

func childIndent(child string) string {
	if child == "" {
		return ""
	}
	return child + "  "
}

func closeLine(buf []byte, child string) []byte {
	buf = append(buf, '\n')
	return append(buf, child[:len(child)-2]...)
}

func appendValueSep(buf []byte, child string) []byte {
	if child == "" {
		return append(buf, ':')
	}
	return append(buf, ':', ' ')
}

const hexDigits = "0123456789abcdef"

// appendString escapes s the way ECMAScript JSON.stringify does: the short
// escapes for ", \, \b, \f, \n, \r, \t, \u00xx for other control bytes, and
// every other rune verbatim.
func appendString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			} else {
				buf = append(buf, c)
			}
		}
	}
	return append(buf, '"')
}

// jsNumber formats f the way ECMAScript Number::toString does (what bun's
// JSON.stringify emits): fixed notation for integral magnitudes below 1e21
// and for decimal points within (-7, 21], shortest round-trip digits with a
// sign-less exponent otherwise; NaN and infinities serialize as null.
func jsNumber(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}
	if f == 0 {
		return "0"
	}
	sign := ""
	abs := f
	if math.Signbit(f) {
		sign = "-"
		abs = -f
	}
	if abs == math.Trunc(abs) && abs < 1e21 {
		return sign + strconv.FormatFloat(abs, 'f', -1, 64)
	}
	sci := strconv.FormatFloat(abs, 'e', -1, 64)
	epos := strings.IndexByte(sci, 'e')
	digits := strings.ReplaceAll(sci[:epos], ".", "")
	exp, _ := strconv.Atoi(sci[epos+1:])
	k := len(digits)
	n := exp + 1
	switch {
	case k <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-k)
	case 0 < n && n < k && n <= 21:
		return sign + digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits
	default:
		rest := ""
		if k > 1 {
			rest = "." + digits[1:]
		}
		e := n - 1
		eSign := "+"
		if e < 0 {
			eSign = "-"
			e = -e
		}
		return sign + digits[:1] + rest + "e" + eSign + strconv.Itoa(e)
	}
}
