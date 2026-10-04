package daemon

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
)

type hello struct {
	Hello   int      `json:"hello"`
	Version string   `json:"version"`
	Profile string   `json:"profile"`
	Sources []string `json:"sources"`
	Only    []string `json:"only"`
	Name    string   `json:"name"`
}

type reply struct {
	OK      bool   `json:"ok"`
	Version string `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
}

type frame struct {
	Line   string `json:"line,omitempty"`
	Ctl    string `json:"ctl,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type command struct {
	Cmd    string `json:"cmd"`
	Reason string `json:"reason,omitempty"`
}

type framer struct{ scanner *bufio.Scanner }

func newFramer(r io.Reader) *framer {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 16*1024*1024)
	return &framer{s}
}

func (f *framer) read(v any) error {
	if !f.scanner.Scan() {
		if err := f.scanner.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	return json.Unmarshal(f.scanner.Bytes(), v)
}

func writeFrame(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

var selections = map[string][]string{
	"listen": {"telegram", "discord"},
	"google": {"google"},
	"remind": {"remind"},
	"herdr":  {"herdr"},
	"rpc":    {"rpc"},
	"tidy":   {"tidy"},
	"all":    {"telegram", "discord", "google", "remind", "herdr", "rpc", "tidy"},
}

func parseAttach(args []string, version string) (hello, error) {
	h := hello{Hello: 1, Version: version, Profile: "main"}
	if len(args) == 0 {
		return h, fmt.Errorf("attach requires a source")
	}
	names, ok := selections[args[0]]
	if !ok {
		return h, fmt.Errorf("unknown source %s", args[0])
	}
	h.Sources = slices.Clone(names)
	for i := 1; i < len(args); i++ {
		key, value, inline := strings.Cut(args[i], "=")
		switch key {
		case "--profile", "--only", "--name":
			if !inline {
				i++
				if i == len(args) {
					return h, fmt.Errorf("%s requires a value", key)
				}
				value = args[i]
			}
			if value == "" {
				return h, fmt.Errorf("%s requires a nonempty value", key)
			}
			switch key {
			case "--profile":
				h.Profile = value
			case "--name":
				h.Name = value
			case "--only":
				h.Only = strings.Split(value, ",")
			}
		default:
			return h, fmt.Errorf("unexpected attach argument %s", args[i])
		}
	}
	if h.Name == "" {
		h.Name = args[0] + "-" + h.Profile
	}
	return h, nil
}

func matches(h hello, profile, source, line string) bool {
	if h.Profile != profile {
		return false
	}
	if source == "" || strings.HasPrefix(line, "LOG omosense ") {
		return true // Daemon notices are profile-wide, regardless of --only.
	}
	prefix, _, _ := strings.Cut(line, " ")
	return slices.Contains(h.Sources, source) && (h.Only == nil || slices.Contains(h.Only, prefix))
}
