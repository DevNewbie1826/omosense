package core

import "testing"

// TestThreadActive pins the IS-4 closed rule: only a done/closed status
// string or a present non-empty string closed value deactivates an entry.
func TestThreadActive(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want bool
	}{
		{"empty entry", `{}`, true},
		{"status active", `{"status":"active"}`, true},
		{"status working", `{"status":"working"}`, true},
		{"status standing", `{"status":"standing"}`, true},
		{"status done", `{"status":"done"}`, false},
		{"status closed", `{"status":"closed"}`, false},
		{"status is case-sensitive", `{"status":"Done"}`, true},
		{"status non-string", `{"status":5}`, true},
		{"closed empty string", `{"closed":""}`, true},
		{"closed null", `{"closed":null}`, true},
		{"closed non-string", `{"closed":123}`, true},
		{"closed set", `{"closed":"2026-10-07T00:00:00.000Z"}`, false},
		{"closed set beats active status", `{"status":"active","closed":"2026-10-07T00:00:00.000Z"}`, false},
		{"done status beats empty closed", `{"status":"done","closed":""}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ParseJSON([]byte(tc.doc))
			if err != nil {
				t.Fatal(err)
			}
			if got := ThreadActive(v.(*OMap)); got != tc.want {
				t.Errorf("ThreadActive(%s) = %v, want %v", tc.doc, got, tc.want)
			}
		})
	}
	if !ThreadActive(nil) {
		t.Errorf("ThreadActive(nil) = false, want true (a nil entry is not closed)")
	}
}
