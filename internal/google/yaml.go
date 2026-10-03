package google

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/DevNewbie1826/omosense/internal/core"
	"gopkg.in/yaml.v3"
)

// parseYAML parses zele's YAML output into core values (*core.OMap, []any,
// string, bool, float64, nil) reproducing Bun.YAML.parse scalar semantics:
// the YAML 1.2 core schema plus Bun's signed 0x/0o integer forms. yaml.v3
// supplies the document structure only; its own plain-scalar resolution is
// YAML 1.1 flavored (010 -> octal, 1_000 -> int, yes -> bool) and is
// deliberately ignored in favor of the rules below.
func parseYAML(data []byte) (any, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.Kind == 0 || len(doc.Content) == 0 {
		return nil, nil
	}
	return convertNode(doc.Content[0])
}

// convertNode turns a yaml.v3 node tree into core values, preserving
// mapping key order.
func convertNode(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return convertNode(n.Content[0])
	case yaml.AliasNode:
		if n.Alias == nil {
			return nil, fmt.Errorf("yaml: unresolved alias %q", n.Value)
		}
		return convertNode(n.Alias)
	case yaml.ScalarNode:
		return scalarValue(n), nil
	case yaml.MappingNode:
		m := core.NewOMap()
		for i := 0; i+1 < len(n.Content); i += 2 {
			kv, err := convertNode(n.Content[i])
			if err != nil {
				return nil, err
			}
			v, err := convertNode(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			key, ok := kv.(string)
			if !ok {
				if kv == nil {
					key = "null"
				} else {
					key = jsKeyString(kv)
				}
			}
			m.Set(key, v)
		}
		return m, nil
	case yaml.SequenceNode:
		arr := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := convertNode(c)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("yaml: unsupported node kind %d", n.Kind)
	}
}

// jsKeyString formats a resolved non-string mapping key the way a JS
// object key would be stringified.
func jsKeyString(v any) string {
	switch x := v.(type) {
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// scalarValue resolves a scalar node the way Bun.YAML.parse does: quoted
// and block scalars are strings (yaml.v3 already unescaped the value), an
// explicit !!str tag wins (Bun honors it while ignoring !!int/!!bool), and
// every other plain scalar is resolved from its text by the core-schema
// regexes in resolvePlain.
func scalarValue(n *yaml.Node) any {
	if n.Style&(yaml.SingleQuotedStyle|yaml.DoubleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
		return n.Value
	}
	if n.Tag == "!!str" {
		return n.Value
	}
	return resolvePlain(n.Value)
}

var (
	rePlainNull  = regexp.MustCompile(`^(~|null|Null|NULL|)$`)
	rePlainBool  = regexp.MustCompile(`^(true|True|TRUE|false|False|FALSE)$`)
	rePlainInt   = regexp.MustCompile(`^[-+]?[0-9]+$`)
	rePlainOct   = regexp.MustCompile(`^[-+]?0o[0-7]+$`)
	rePlainHex   = regexp.MustCompile(`^[-+]?0x[0-9a-fA-F]+$`)
	rePlainFloat = regexp.MustCompile(`^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`)
	rePlainInf   = regexp.MustCompile(`^[-+]?(\.inf|\.Inf|\.INF)$`)
	rePlainNan   = regexp.MustCompile(`^(\.nan|\.NaN|\.NAN)$`)
)

// resolvePlain applies the Bun plain-scalar rules in order: null, bool,
// decimal int (leading zeros allowed, 010 -> 10), 0o octal and 0x hex with
// sign, float, and the non-finite .inf/.nan forms which serialize as JSON
// null. Everything else (1_000, 2026-10-05, yes) stays a string. Numbers
// follow JS semantics: float64, so integers beyond 2^53 round.
func resolvePlain(s string) any {
	switch {
	case rePlainNull.MatchString(s):
		return nil
	case rePlainBool.MatchString(s):
		return s == "true" || s == "True" || s == "TRUE"
	case rePlainInt.MatchString(s):
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s
		}
		return f
	case rePlainOct.MatchString(s):
		return radixNumber(s, 2, 8) // skip the "0o" prefix after the sign
	case rePlainHex.MatchString(s):
		return radixNumber(s, 2, 16)
	case rePlainFloat.MatchString(s):
		f, err := strconv.ParseFloat(strings.TrimSuffix(s, "."), 64)
		if err != nil {
			return s
		}
		return f
	case rePlainInf.MatchString(s) || rePlainNan.MatchString(s):
		return nil
	default:
		return s
	}
}

func radixNumber(s string, prefixLen, base int) any {
	neg := false
	if s[0] == '-' || s[0] == '+' {
		neg = s[0] == '-'
		s = s[1:]
	}
	u, err := strconv.ParseUint(s[prefixLen:], base, 64)
	if err != nil {
		return s
	}
	f := float64(u)
	if neg {
		f = -f
	}
	return f
}
