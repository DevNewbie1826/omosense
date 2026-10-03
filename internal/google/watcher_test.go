package google

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func fakeZele(cal, mail []byte, calls *[]string) zeleFunc {
	return func(args []string) zeleResult {
		if calls != nil {
			*calls = append(*calls, strings.Join(args, " "))
		}
		var doc []byte
		switch args[0] {
		case "cal":
			doc = cal
		case "mail":
			doc = mail
		default:
			return zeleResult{code: 1, stderr: "unexpected subcommand " + args[0]}
		}
		if doc == nil {
			return zeleResult{code: 1, stderr: "no fixture"}
		}
		return zeleResult{stdout: string(doc)}
	}
}

func newTestCtx(dir string, prof core.Profile, out *core.Out) *core.Ctx {
	return &core.Ctx{State: dir, Profile: prof, Out: out}
}

func linesOf(buf *bytes.Buffer) []string {
	var out []string
	for _, l := range strings.Split(buf.String(), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func linesWith(buf *bytes.Buffer, prefix string) []string {
	var out []string
	for _, l := range linesOf(buf) {
		if prefix == "LOG" && strings.HasPrefix(l, "LOG ") {
			out = append(out, l)
		} else if prefix != "LOG" && strings.HasPrefix(l, prefix+" ") {
			out = append(out, l)
		}
	}
	return out
}

func payloadOf(t *testing.T, line string) *core.OMap {
	t.Helper()
	i := strings.IndexByte(line, ' ')
	if i < 0 {
		t.Fatalf("line %q has no payload", line)
	}
	v, err := core.ParseJSON([]byte(line[i+1:]))
	if err != nil {
		t.Fatalf("payload of %q: %v", line, err)
	}
	m, ok := v.(*core.OMap)
	if !ok {
		t.Fatalf("payload of %q is not an object", line)
	}
	return m
}

// TestCalendarOnceThenSeenKeys pins CAL dedup and the byte-exact bun seen
// keys (id@Date.parse-ms) computed from the masked fixture.
func TestCalendarOnceThenSeenKeys(t *testing.T) {
	mustSeoul(t)
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	dir := t.TempDir()
	w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(fixture(t, "cal.yaml"), nil, nil)
	w.now = func() time.Time { return time.Date(2026, 10, 6, 9, 10, 0, 0, time.Local) }
	if err := w.calendar(); err != nil {
		t.Fatalf("calendar: %v", err)
	}
	cals := linesWith(&buf, "CAL")
	if len(cals) != 4 {
		t.Fatalf("CAL lines = %d, want 4 (duplicate id+start deduped):\n%s", len(cals), buf.String())
	}
	soons := linesWith(&buf, "SOON")
	if len(soons) != 1 || !strings.Contains(soons[0], "20261006_") {
		t.Fatalf("SOON lines = %v, want exactly the Oct 6 9:30 AM event 20min ahead:\n%s", soons, buf.String())
	}
	if !w.seen.Has("cal:20261005_a3v3ietd9obqpnrv5hg4emhmvc@google.com@1002207600000") {
		t.Errorf("bun-compatible cal key missing; have %v", w.seen.Keys())
	}
	soonID, _ := payloadOf(t, soons[0]).Get("id")
	if !w.seen.Has(fmt.Sprintf("soon:%s@1002328200000", soonID)) {
		t.Errorf("bun-compatible soon key missing for %v; have %v", soonID, w.seen.Keys())
	}
	buf.Reset()
	if err := w.calendar(); err != nil {
		t.Fatalf("calendar 2: %v", err)
	}
	if n := len(linesOf(&buf)); n != 0 {
		t.Errorf("second calendar pass emitted %d lines, want 0:\n%s", n, buf.String())
	}
}

func soonFires(t *testing.T, dir string, now time.Time) int {
	t.Helper()
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(fixture(t, "cal.yaml"), nil, nil)
	w.now = func() time.Time { return now }
	if err := w.calendar(); err != nil {
		t.Fatal(err)
	}
	return len(linesWith(&buf, "SOON"))
}

// TestSOONWindowBoundaries pins the fixed SOON timing: fire strictly inside
// the 30 minute window including its edge, never at or past the start.
func TestSOONWindowBoundaries(t *testing.T) {
	loc := mustSeoul(t)
	start := time.Date(2026, 10, 6, 9, 30, 0, 0, loc)
	cases := []struct {
		off  time.Duration
		want int
	}{
		{20 * time.Minute, 1},
		{30 * time.Minute, 1},
		{30*time.Minute + time.Second, 0},
		{0, 0},
		{-time.Minute, 0},
	}
	for _, c := range cases {
		dir := t.TempDir()
		if got := soonFires(t, dir, start.Add(-c.off)); got != c.want {
			t.Errorf("SOON count with start %v after now = %d, want %d", c.off, got, c.want)
		}
	}
}

// TestMailSilentNoiseTrunc pins mail(firstRun) silence, the NOISE filter,
// the MAIL payload shape (absent from/subject omitted, rune-truncated
// snippet) and the mail:<account>:<id> keys.
func TestMailSilentNoiseTrunc(t *testing.T) {
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	dir := t.TempDir()
	mk := func() *watcher {
		t.Helper()
		buf.Reset()
		w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, true)
		if err != nil {
			t.Fatal(err)
		}
		w.zele = fakeZele(nil, fixture(t, "mail.yaml"), nil)
		w.now = func() time.Time { return time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC) }
		return w
	}
	w := mk()
	if err := w.mail(true); err != nil {
		t.Fatal(err)
	}
	if got := linesWith(&buf, "MAIL"); len(got) != 0 {
		t.Fatalf("silent first-run mail printed %d MAIL lines, want 0", len(got))
	}
	if n := w.seen.Len(); n != 5 {
		t.Fatalf("silent mail seeded %d seen keys, want 5: %v", n, w.seen.Keys())
	}
	w2 := mk()
	if err := w2.mail(false); err != nil {
		t.Fatal(err)
	}
	mails := linesWith(&buf, "MAIL")
	if len(mails) != 4 {
		t.Fatalf("MAIL lines = %d, want 4 (NOISE welcome filtered):\n%s", len(mails), buf.String())
	}
	p := payloadOf(t, mails[3])
	if got := p.Keys(); !reflect.DeepEqual(got, []string{"account", "id", "from", "subject", "snippet"}) {
		t.Errorf("MAIL payload keys = %v", got)
	}
	snip, _ := p.Get("snippet")
	if n := len([]rune(snip.(string))); n != 200 {
		t.Errorf("snippet runes = %d, want 200 (rune truncation)", n)
	}
	if !w2.seen.Has("mail:fixturemain@gmail.com:1a10300000000001") {
		t.Errorf("NOISE mail not marked seen")
	}
	for _, l := range mails {
		if strings.Contains(l, "Welcome") {
			t.Errorf("NOISE mail printed: %s", l)
		}
	}
}

// TestMailFieldOmission pins MAIL payload omission: absent from/subject
// keys stay absent and a missing snippet becomes the empty string.
func TestMailFieldOmission(t *testing.T) {
	doc := "summary: 1 threads (inbox)\nitems:\n  - account: a@masked.example\n    id: ffeeddccbbaa0099\n    date: 1h ago\n"
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(t.TempDir(), core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(nil, []byte(doc), nil)
	w.now = time.Now
	if err := w.mail(false); err != nil {
		t.Fatal(err)
	}
	mails := linesWith(&buf, "MAIL")
	if len(mails) != 1 {
		t.Fatalf("MAIL lines = %d, want 1:\n%s", len(mails), buf.String())
	}
	p := payloadOf(t, mails[0])
	if got := p.Keys(); !reflect.DeepEqual(got, []string{"account", "id", "snippet"}) {
		t.Errorf("payload keys = %v, want [account id snippet]", got)
	}
	sn, _ := p.Get("snippet")
	if sn != "" {
		t.Errorf("snippet = %#v, want empty string", sn)
	}
}

// TestPruneDropsOldSeen pins the 7-day cutoff: strictly older values are
// dropped, the exact edge and newer survive, key order preserved.
func TestPruneDropsOldSeen(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cut := now.Add(-7 * 24 * time.Hour)
	w := &watcher{seen: core.NewOMap(), now: func() time.Time { return now }}
	w.seen.Set("old", now.Add(-8*24*time.Hour).UnixMilli())
	w.seen.Set("keep", now.Add(-24*time.Hour).UnixMilli())
	w.seen.Set("edge", cut.UnixMilli())
	w.prune()
	if got := w.seen.Keys(); !reflect.DeepEqual(got, []string{"keep", "edge"}) {
		t.Errorf("after prune keys = %v, want [keep edge]", got)
	}
}

// TestZeleFailureLogs pins the zele error grammar, the rune truncation of
// stderr, and that a failed zele run yields an empty item list without
// failing the tick.
func TestZeleFailureLogs(t *testing.T) {
	long := strings.Repeat("에러", 150)
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(t.TempDir(), core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = func(args []string) zeleResult { return zeleResult{code: 2, stderr: long} }
	w.now = time.Now
	if err := w.calendar(); err != nil {
		t.Fatalf("calendar returned %v, want nil (zele failure is logged, not an error)", err)
	}
	logs := linesWith(&buf, "LOG")
	want := "LOG zele error cal events --all --days 2 --limit 50: " + strings.Repeat("에러", 100)
	if len(logs) != 1 || logs[0] != want {
		t.Errorf("LOG = %q, want [%q]", logs, want)
	}
	if c := len(linesWith(&buf, "CAL")); c != 0 {
		t.Errorf("CAL lines = %d, want 0", c)
	}
}

// TestOnceReadOnly pins --once: dedup against existing seen, no state
// writes, no lock file, no starting LOG.
func TestOnceReadOnly(t *testing.T) {
	mustSeoul(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "google-seen-main.json")
	seed := []byte(`{"mail:fixturemain@gmail.com:1a10300000000001":100,"cal:20261005_a3v3ietd9obqpnrv5hg4emhmvc@google.com@1002207600000":100}`)
	if err := os.WriteFile(path, seed, 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, false)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(fixture(t, "cal.yaml"), fixture(t, "mail.yaml"), nil)
	w.now = func() time.Time { return time.Date(2026, 10, 3, 19, 0, 0, 0, time.Local) }
	w.once()
	if cals := len(linesWith(&buf, "CAL")); cals != 3 {
		t.Fatalf("CAL lines = %d, want 3 (seeded key deduped):\n%s", cals, buf.String())
	}
	if mails := len(linesWith(&buf, "MAIL")); mails != 4 {
		t.Fatalf("MAIL lines = %d, want 4 (seeded noise mail deduped):\n%s", mails, buf.String())
	}
	if got := linesWith(&buf, "LOG"); len(got) != 0 {
		t.Errorf("LOG lines = %v, want none in --once", got)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(seed) {
		t.Errorf("seen file changed by --once: %q", after)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 || ents[0].Name() != "google-seen-main.json" {
		t.Errorf("state dir entries = %v, want only the seen file (no lock, no writes)", ents)
	}
}

// TestOnceFamilyNoMail pins --once with mail:false: no mail subprocess at
// all and no MAIL lines, while the calendar still runs.
func TestOnceFamilyNoMail(t *testing.T) {
	mustSeoul(t)
	var calls []string
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(t.TempDir(), core.Profile{Name: "family", Mail: false}, sink), sink, false)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(fixture(t, "cal.yaml"), fixture(t, "mail.yaml"), &calls)
	w.now = func() time.Time { return time.Date(2026, 10, 3, 19, 0, 0, 0, time.Local) }
	w.once()
	if cals := len(linesWith(&buf, "CAL")); cals != 4 {
		t.Fatalf("CAL lines = %d, want 4:\n%s", cals, buf.String())
	}
	if mails := len(linesWith(&buf, "MAIL")); mails != 0 {
		t.Errorf("MAIL lines = %d, want 0 with mail:false", mails)
	}
	for _, c := range calls {
		if strings.HasPrefix(c, "mail") {
			t.Errorf("mail subprocess ran with mail:false: %q", c)
		}
	}
}

// TestLoopTicks pins the watch-google.ts loop: pre-loop silent firstRun
// mail, calendar on tick 0 and 5, mail on later ticks, prune and save every
// tick, 60s sleeps, clean exit on cancel, and the exact saved key order.
func TestLoopTicks(t *testing.T) {
	mustSeoul(t)
	dir := t.TempDir()
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(fixture(t, "cal.yaml"), fixture(t, "mail.yaml"), nil)
	w.now = func() time.Time { return time.Date(2026, 10, 3, 19, 0, 0, 0, time.Local) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sleeps := 0
	var slept []time.Duration
	w.sleep = func(c context.Context, d time.Duration) error {
		sleeps++
		slept = append(slept, d)
		if sleeps >= 6 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	if err := w.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sleeps != 6 {
		t.Fatalf("sleeps = %d, want 6", sleeps)
	}
	for _, d := range slept {
		if d != time.Minute {
			t.Errorf("slept %v, want 1m", d)
		}
	}
	lines := linesOf(&buf)
	if len(lines) == 0 || lines[0] != "LOG google watcher starting (profile main)" {
		t.Errorf("first line = %v, want the starting LOG", lines)
	}
	if cals := len(linesWith(&buf, "CAL")); cals != 4 {
		t.Errorf("CAL lines = %d, want 4 (tick 0 and tick 5, deduped)", cals)
	}
	if mails := len(linesWith(&buf, "MAIL")); mails != 0 {
		t.Errorf("MAIL lines = %d, want 0 (firstRun seeds silently)", mails)
	}
	b, err := os.ReadFile(filepath.Join(dir, "google-seen-main.json"))
	if err != nil {
		t.Fatalf("seen file not written: %v", err)
	}
	if bytes.ContainsAny(b, " \n") {
		t.Errorf("seen file is not compact: %q", b)
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	keys := v.(*core.OMap).Keys()
	want := []string{
		"mail:fixturemain@gmail.com:1a1024917255ca5b",
		"mail:fixturemain@gmail.com:1a0ffa63888a1460",
		"mail:fixturemain@gmail.com:1a0ff41e81e22bc6",
		"mail:fixturemain@gmail.com:1a10300000000001",
		"mail:fixturemain@gmail.com:1a10300000000002",
		"cal:20261005_a3v3ietd9obqpnrv5hg4emhmvc@google.com@1002207600000",
	}
	if len(keys) < len(want) || !reflect.DeepEqual(keys[:len(want)], want) {
		t.Errorf("seen key order = %v, want prefix %v", keys, want)
	}
}

// TestLoopZeleParseErrorSkipsIteration pins the TS try-block shape: a YAML
// parse failure logs "google watcher <err>" and skips the rest of that
// tick (no save), while later ticks recover and save.
func TestLoopZeleParseErrorSkipsIteration(t *testing.T) {
	mustSeoul(t)
	dir := t.TempDir()
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(dir, core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = func(args []string) zeleResult {
		if args[0] == "cal" {
			return zeleResult{stdout: "summary: x\nitems: [1, 2\n"}
		}
		return zeleResult{stdout: string(fixture(t, "mail.yaml"))}
	}
	w.now = func() time.Time { return time.Date(2026, 10, 3, 19, 0, 0, 0, time.Local) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sleeps := 0
	w.sleep = func(c context.Context, d time.Duration) error {
		sleeps++
		if sleeps >= 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	if err := w.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	logs := linesWith(&buf, "LOG")
	if len(logs) != 2 || logs[1] == "LOG google watcher starting (profile main)" {
		t.Errorf("LOG lines = %q, want starting plus one parse-error line", logs)
	}
	if cals := len(linesWith(&buf, "CAL")); cals != 0 {
		t.Errorf("CAL = %d, want 0", cals)
	}
	if _, err := os.Stat(filepath.Join(dir, "google-seen-main.json")); err != nil {
		t.Errorf("seen file missing after the recovered tick 1: %v", err)
	}
}

// TestStartMap pins start-as-map items: dateTime drives the key and the
// SOON instant, date-only gets no SOON, and an empty map yields the NaN
// key.
func TestStartMap(t *testing.T) {
	loc := mustSeoul(t)
	doc := []byte(`summary: 3 events
items:
  - account: a@masked.example
    id: mapdt@google.com
    summary: timed
    start:
      dateTime: 2026-10-06T09:30:00+09:00
  - account: a@masked.example
    id: mapdate@google.com
    summary: allday
    start:
      date: 2026-10-06
  - account: a@masked.example
    id: mapnone@google.com
    summary: empty
    start: {}
`)
	var buf bytes.Buffer
	sink := core.NewOut(&buf)
	w, err := newWatcher(newTestCtx(t.TempDir(), core.Profile{Name: "main", Mail: true}, sink), sink, true)
	if err != nil {
		t.Fatal(err)
	}
	w.zele = fakeZele(doc, nil, nil)
	w.now = func() time.Time { return time.Date(2026, 10, 6, 9, 10, 0, 0, loc) }
	if err := w.calendar(); err != nil {
		t.Fatal(err)
	}
	if cals := len(linesWith(&buf, "CAL")); cals != 3 {
		t.Fatalf("CAL = %d, want 3:\n%s", cals, buf.String())
	}
	soons := linesWith(&buf, "SOON")
	if len(soons) != 1 || !strings.Contains(soons[0], "mapdt") {
		t.Fatalf("SOON = %v, want only the dateTime event", soons)
	}
	dt := time.Date(2026, 10, 6, 9, 30, 0, 0, loc).UnixMilli()
	d := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, key := range []string{
		"cal:mapdt@google.com@" + fmt.Sprint(dt),
		"cal:mapdate@google.com@" + fmt.Sprint(d),
		"cal:mapnone@google.com@NaN",
	} {
		if !w.seen.Has(key) {
			t.Errorf("seen key %q missing; have %v", key, w.seen.Keys())
		}
	}
}

// TestSourcesMetadata pins the daemon contract: one pausable google source
// with the watch-google-<profile> lock and the legacy watch-google
// fallback.
func TestSourcesMetadata(t *testing.T) {
	for _, prof := range []string{"main", "family"} {
		srcs := Sources(&core.Ctx{Profile: core.Profile{Name: prof}})
		if len(srcs) != 1 {
			t.Fatalf("profile %s: Sources = %d, want 1", prof, len(srcs))
		}
		s := srcs[0]
		if s.Name() != "google" {
			t.Errorf("name = %q", s.Name())
		}
		if !reflect.DeepEqual(s.Prefixes(), []string{"CAL", "SOON", "MAIL"}) {
			t.Errorf("prefixes = %v", s.Prefixes())
		}
		if s.AlwaysOn() {
			t.Errorf("google source must be pausable (pause-while-idle)")
		}
		name, legacy := s.LockName()
		if name != "watch-google-"+prof || legacy != "watch-google" {
			t.Errorf("lock = %q/%q, want watch-google-%s/watch-google", name, legacy, prof)
		}
	}
}

// TestRunOnceCompatReadOnly drives the real compat entry point: --once
// under a family profile (mail:false) loads read-only, prints CAL only and
// writes nothing.
func TestRunOnceCompatReadOnly(t *testing.T) {
	mustSeoul(t)
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := []byte(`{"telegram":{"bot":"t"},"profiles":{"family":{"discord":true,"telegram":["t"],"mail":false}}}`)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), cfg, 0o644); err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"mail:fixturemain@gmail.com:1a10300000000001":100}`)
	seenPath := filepath.Join(state, "google-seen-family.json")
	if err := os.WriteFile(seenPath, seed, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMOMEOW_DIR", dir)
	t.Setenv("OMOMEOW_STATE", state)
	pa := core.ParseArgs([]string{"--profile", "family", "--once"})
	ctx, err := core.Load(pa, false)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	ctx.Out = core.NewOut(&buf)
	old := execZele
	execZele = fakeZele(fixture(t, "cal.yaml"), fixture(t, "mail.yaml"), nil)
	defer func() { execZele = old }()
	if code := Run(ctx, []string{"--profile", "family", "--once"}); code != 0 {
		t.Fatalf("Run --once exit = %d, want 0", code)
	}
	if cals := len(linesWith(&buf, "CAL")); cals != 4 {
		t.Fatalf("CAL lines = %d, want 4:\n%s", cals, buf.String())
	}
	if mails := len(linesWith(&buf, "MAIL")); mails != 0 {
		t.Errorf("MAIL lines = %d, want 0 (family has mail:false)", mails)
	}
	after, err := os.ReadFile(seenPath)
	if err != nil || string(after) != string(seed) {
		t.Errorf("family seen file changed by --once: %q", after)
	}
	ents, _ := os.ReadDir(state)
	if len(ents) != 1 {
		t.Errorf("state entries = %d, want only the seen file (no lock, no writes): %v", len(ents), ents)
	}
}
