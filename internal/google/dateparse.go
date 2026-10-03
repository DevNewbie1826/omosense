package google

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// legacyYear is the year JS Date.parse assigns to yearless dates (the
// ECMAScript legacy parser default), which is why the bun watcher's SOON
// never fired: zele prints "Oct 5"-style starts.
const legacyYear = 2001

const monthAlternation = `Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December`

var monthNames = map[string]time.Month{
	"Jan": time.January, "Feb": time.February, "Mar": time.March, "Apr": time.April,
	"May": time.May, "Jun": time.June, "Jul": time.July, "Aug": time.August,
	"Sep": time.September, "Oct": time.October, "Nov": time.November, "Dec": time.December,
	"January": time.January, "February": time.February, "March": time.March,
	"April": time.April, "June": time.June, "July": time.July, "August": time.August,
	"September": time.September, "October": time.October, "November": time.November,
	"December": time.December,
}

var (
	reDateOnly = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	reMonDay   = regexp.MustCompile(`^(` + monthAlternation + `) ([0-9]{1,2})$`)
	// reMonDayTime matches the timed zele forms: "Oct 6, 9:30 AM",
	// "Oct 5 9:30 AM", "Oct 5 12:05 am", "Oct 7, 10:00 PM" with either a
	// space or U+202F before the meridiem, and the 24h "Oct 8, 21:30".
	reMonDayTime = regexp.MustCompile(
		"^(" + monthAlternation + ") ([0-9]{1,2})(?:, | )([0-9]{1,2}):([0-9]{2})(?:[ \u202f]?(AM|PM|am|pm))?$")
)

// parseISOInstant parses the ISO forms that carry a real instant:
// RFC3339 (zone mandatory) and zone-less datetimes in time.Local.
func parseISOInstant(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// parseInstant is the JS Date.parse compatibility used for seen keys:
// RFC3339 instants, UTC-midnight date-only forms, local zone-less ISO
// datetimes, and the zele yearless forms in 2001 local time (time.Date
// normalizes overflow, so "Feb 29" becomes March 1 like JS). Full month
// names, an optional comma and a lowercase meridiem are accepted because
// the JS parser accepts them.
func parseInstant(s string) (time.Time, bool) {
	if t, ok := parseISOInstant(s); ok {
		return t, true
	}
	if reDateOnly.MatchString(s) {
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t, true
		}
		return time.Time{}, false
	}
	if m := reMonDay.FindStringSubmatch(s); m != nil {
		return time.Date(legacyYear, monthNames[m[1]], atoi(m[2]), 0, 0, 0, 0, time.Local), true
	}
	if m := reMonDayTime.FindStringSubmatch(s); m != nil {
		return time.Date(legacyYear, monthNames[m[1]], atoi(m[2]),
			hourOfDay(atoi(m[3]), m[5]), atoi(m[4]), 0, 0, time.Local), true
	}
	return time.Time{}, false
}

// hourOfDay applies the 12h meridiem: 12 AM is midnight, 12 PM is noon.
func hourOfDay(h int, meridiem string) int {
	if meridiem == "" {
		return h
	}
	if h == 12 {
		h = 0
	}
	if strings.EqualFold(meridiem, "pm") {
		h += 12
	}
	return h
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// startMs converts a zele start string to JS Date.parse milliseconds; the
// boolean reports non-NaN.
func startMs(s string) (float64, bool) {
	if t, ok := parseInstant(s); ok {
		return float64(t.UnixMilli()), true
	}
	return 0, false
}

// msKeyText renders the seen-key timestamp half: decimal milliseconds, or
// the literal "NaN" for unparseable starts, exactly like the bun template
// string `${Date.parse(x)}`.
func msKeyText(ms float64, ok bool) string {
	if !ok {
		return "NaN"
	}
	return strconv.FormatFloat(ms, 'f', -1, 64)
}

// realStart is the year-corrected instant used for SOON timing (the user
// decision that fixed the never-firing bun SOON): timed zele formats infer
// the calendar year from {now-1, now, now+1} closest to now; RFC3339 and
// zone-less ISO datetimes keep their parsed instant; date-only, all-day
// and unparseable starts report false, so they never fire SOON.
func realStart(s string, now time.Time) (time.Time, bool) {
	if m := reMonDayTime.FindStringSubmatch(s); m != nil {
		month, day := monthNames[m[1]], atoi(m[2])
		hour, min := hourOfDay(atoi(m[3]), m[5]), atoi(m[4])
		var best time.Time
		for i, y := range []int{now.Year() - 1, now.Year(), now.Year() + 1} {
			t := time.Date(y, month, day, hour, min, 0, 0, time.Local)
			if i == 0 || absDuration(t.Sub(now)) < absDuration(best.Sub(now)) {
				best = t
			}
		}
		return best, true
	}
	return parseISOInstant(s)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
