package tidy

// The embedded tzdata keeps Asia/Seoul resolvable without system timezone
// data (plan requirement).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/DevNewbie1826/omosense/internal/core"
)

var dateDir = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// seoulDate renders now as YYYY-MM-DD in Asia/Seoul, matching
// Intl.DateTimeFormat("en-CA", {timeZone: "Asia/Seoul", year: "numeric",
// month: "2-digit", day: "2-digit"}).
func seoulDate(now time.Time) string {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		loc = time.FixedZone("KST", 9*60*60)
	}
	return now.In(loc).Format("2006-01-02")
}

func lastBackupDate(w *core.OMap) (string, bool) {
	v, _ := w.Get("lastBackupDate")
	s, ok := v.(string)
	return s, ok
}

// backup runs the daily git-bundle backup (memory-tidy.ts:43-80): every
// agent repo under AGENTS, including the profile memory repo and names in
// tidy.exclude (heads() skips those; backup does not), qualified by
// `git rev-parse --git-dir` rather than a .git entry, is bundled and
// verified into ~/.omo/memory-backups/<Seoul date>/ (mode 0700); failed
// bundles are removed and named in failed=, only the newest 14 date dirs
// survive, and lastBackupDate is stamped through a FRESH watermark read.
// When the date already ran: quietIfDone returns silently, otherwise the
// already-ran-today LOG line prints. The return reports whether every
// bundle succeeded.
func (t *tidyer) backup(ctx context.Context, quietIfDone bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	date := seoulDate(t.now())
	w, err := t.readWatermark()
	if err != nil {
		return false, err
	}
	if s, ok := lastBackupDate(w); ok && s == date {
		if !quietIfDone {
			t.sink.Log(fmt.Sprintf("memory-tidy backup %s already ran today", date))
		}
		return true, nil
	}
	dir := filepath.Join(t.backups, date)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, errors.New("Error: " + err.Error())
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return false, errors.New("Error: " + err.Error())
	}
	names, err := sortedAgents(t.agents)
	if err != nil {
		return false, err
	}
	repos := 0
	var bytes int64
	var failed []string
	for _, name := range names {
		repo := filepath.Join(t.agents, name, "repo")
		if _, err := os.Stat(repo); err != nil {
			continue
		}
		if _, _, code, err := runGit(ctx, "-C", repo, "rev-parse", "--git-dir"); err != nil {
			return false, err
		} else if code != 0 {
			continue
		}
		repos++
		bundle := filepath.Join(dir, name+".bundle")
		_, _, ccode, err := runGit(ctx, "-C", repo, "bundle", "create", bundle, "--all")
		if err != nil {
			return false, err
		}
		verified := ccode == 0
		if verified {
			if _, _, vcode, err := runGit(ctx, "-C", repo, "bundle", "verify", bundle); err != nil {
				return false, err
			} else if vcode != 0 {
				verified = false
			}
		}
		if !verified {
			failed = append(failed, name)
			_ = os.Remove(bundle)
			continue
		}
		fi, err := os.Stat(bundle)
		if err != nil {
			return false, errors.New("Error: " + err.Error())
		}
		bytes += fi.Size()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := pruneBackups(t.backups); err != nil {
		return false, err
	}
	if err := t.updateWatermark(func(w *core.OMap) error {
		w.Set("lastBackupDate", date)
		return nil
	}); err != nil {
		return false, err
	}
	failedStr := strings.Join(failed, ",")
	if failedStr == "" {
		failedStr = "none"
	}
	t.sink.Log(fmt.Sprintf("memory-tidy backup %s repos=%d bytes=%d failed=%s", date, repos, bytes, failedStr))
	return len(failed) == 0, nil
}

// pruneBackups keeps only the newest 14 YYYY-MM-DD directories, like
// days.slice(14) over the reverse-sorted day list.
func pruneBackups(backups string) error {
	entries, err := os.ReadDir(backups)
	if err != nil {
		return errors.New("Error: " + err.Error())
	}
	var days []string
	for _, e := range entries {
		if e.IsDir() && dateDir.MatchString(e.Name()) {
			days = append(days, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(days)))
	for _, day := range days[min(14, len(days)):] {
		if err := os.RemoveAll(filepath.Join(backups, day)); err != nil {
			return errors.New("Error: " + err.Error())
		}
	}
	return nil
}
