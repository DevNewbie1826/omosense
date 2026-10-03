package say

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// truthy reports the ECMAScript truthiness of v, which say.ts relies on for
// `...(a.thread_id ? {...} : {})`-style spreads.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := x.Float64()
		return err == nil && f != 0
	default:
		return true
	}
}

// compactJSON renders any parsed JSON value (OMap, []any, scalar) as compact
// JSON, byte-compatible with JSON.stringify.
func compactJSON(v any) ([]byte, error) {
	if m, ok := v.(*core.OMap); ok {
		return m.Marshal()
	}
	wrapper := core.NewOMap()
	wrapper.Set("v", v)
	b, err := wrapper.Marshal()
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(bytes.TrimPrefix(b, []byte(`{"v":`)), []byte(`}`)), nil
}

// encodeURIComponent escapes s exactly like the JS global (the emoji in
// Discord reaction paths): everything but A-Za-z0-9-_.!~*'() becomes %XX
// UTF-8 bytes.
func encodeURIComponent(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()"
	var b strings.Builder
	for _, r := range s {
		if r < 0x80 && strings.ContainsRune(unreserved, r) {
			b.WriteRune(r)
			continue
		}
		for _, by := range []byte(string(r)) {
			fmt.Fprintf(&b, "%%%02X", by)
		}
	}
	return b.String()
}

// copyKeys copies keys that are present; say.ts object literals omit absent
// keys entirely (undefined), they never become null.
func copyKeys(dst, src *core.OMap, keys ...string) {
	for _, k := range keys {
		if v, ok := src.Get(k); ok {
			dst.Set(k, v)
		}
	}
}

// copyAs copies a present src key under a new name.
func copyAs(dst, src *core.OMap, key, as string) {
	if v, ok := src.Get(key); ok {
		dst.Set(as, v)
	}
}

// setTruthy copies the key only when truthy (JS `...(x ? {x} : {})`).
func setTruthy(dst, src *core.OMap, key, as string) {
	if v, ok := src.Get(key); ok && truthy(v) {
		dst.Set(as, v)
	}
}

// setThread adds message_thread_id when a.thread_id is truthy (say.ts's
// thread() helper).
func setThread(dst, src *core.OMap) {
	setTruthy(dst, src, "thread_id", "message_thread_id")
}

// setReplyParams adds reply_parameters when a.reply_to is truthy.
func setReplyParams(dst, src *core.OMap) {
	v, ok := src.Get("reply_to")
	if !ok || !truthy(v) {
		return
	}
	rp := core.NewOMap()
	rp.Set("message_id", v)
	dst.Set("reply_parameters", rp)
}

// nullishDefault returns src[key] ?? fallback (only null/undefined fall back).
func nullishDefault(src *core.OMap, key string, fallback any) any {
	if v, ok := src.Get(key); ok && v != nil {
		return v
	}
	return fallback
}

// emojiEntry builds the single setMessageReaction reaction element.
func emojiEntry(a *core.OMap) *core.OMap {
	m := core.NewOMap()
	m.Set("type", "emoji")
	m.Set("emoji", nullishDefault(a, "emoji", "👀"))
	return m
}

func strArg(a *core.OMap, key string) string {
	v, _ := a.Get(key)
	s, _ := v.(string)
	return s
}

// jsStr renders a path segment the way a JS template literal interpolates it.
func jsStr(a *core.OMap, key string) string {
	return jsValueStr(mustGet(a, key))
}

func mustGet(a *core.OMap, key string) any {
	v, _ := a.Get(key)
	return v
}

func jsValueStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// emojiStr resolves a.emoji ?? "👀" for path encoding: only a missing or null
// emoji falls back; "" is kept, like JS nullish coalescing.
func emojiStr(a *core.OMap) string {
	if v, ok := a.Get("emoji"); ok && v != nil {
		return jsValueStr(v)
	}
	return "👀"
}
