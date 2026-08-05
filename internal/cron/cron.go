// Package cron parses five-field cron specifications and computes when they
// next fire.
//
// Written by hand rather than pulled in as a dependency: it is a small,
// well-bounded, highly testable problem, and the day-of-month / day-of-week
// rule below is the sort of thing worth understanding rather than trusting.
//
// # Supported syntax
//
//	┌───────────── minute       0-59
//	│ ┌─────────── hour         0-23
//	│ │ ┌───────── day of month 1-31
//	│ │ │ ┌─────── month        1-12 or JAN-DEC
//	│ │ │ │ ┌───── day of week  0-6 or SUN-SAT (0 and 7 are both Sunday)
//	│ │ │ │ │
//	* * * * *
//
// Each field accepts `*`, a single value, a `low-high` range, a `*/step` or
// `low-high/step` interval, and comma-separated lists of any of those.
//
// Descriptors: @yearly, @annually, @monthly, @weekly, @daily, @midnight,
// @hourly, and @every <duration>.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// searchLimit bounds how far Next will look before declaring a spec
// unsatisfiable. "30 February" parses fine and never fires; without a limit the
// scheduler would spin forever on it.
const searchLimit = 5 * 365 * 24 * time.Hour

// Schedule is a parsed cron specification.
//
// Each field is a bitmask over its legal range, which makes matching a single
// shift-and-test and keeps the whole schedule in a handful of words.
type Schedule struct {
	minute uint64 // bits 0-59
	hour   uint64 // bits 0-23
	dom    uint64 // bits 1-31
	month  uint64 // bits 1-12
	dow    uint64 // bits 0-6

	// domStar and dowStar record whether the day fields were literally "*",
	// which the matching rule in dayMatches depends on.
	domStar bool
	dowStar bool

	// every is set only by the @every descriptor, which bypasses field
	// matching entirely.
	every time.Duration
}

type fieldSpec struct {
	name    string
	min     uint
	max     uint
	names   map[string]uint
	starOut *bool
}

var monthNames = map[string]uint{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]uint{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

var descriptors = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// Parse converts a cron specification into a Schedule.
func Parse(spec string) (*Schedule, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("cron: empty specification")
	}

	if strings.HasPrefix(spec, "@every ") {
		d, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(spec, "@every ")))
		if err != nil {
			return nil, fmt.Errorf("cron: parse @every duration: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("cron: @every duration must be positive, got %s", d)
		}
		return &Schedule{every: d}, nil
	}

	if expanded, ok := descriptors[strings.ToLower(spec)]; ok {
		spec = expanded
	}

	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), spec)
	}

	s := &Schedule{}
	specs := []fieldSpec{
		{name: "minute", min: 0, max: 59},
		{name: "hour", min: 0, max: 23},
		{name: "day of month", min: 1, max: 31, starOut: &s.domStar},
		{name: "month", min: 1, max: 12, names: monthNames},
		{name: "day of week", min: 0, max: 6, names: dayNames, starOut: &s.dowStar},
	}
	targets := []*uint64{&s.minute, &s.hour, &s.dom, &s.month, &s.dow}

	for i, f := range specs {
		bits, err := parseField(fields[i], f)
		if err != nil {
			return nil, err
		}
		*targets[i] = bits
		if f.starOut != nil {
			*f.starOut = fields[i] == "*"
		}
	}

	return s, nil
}

// parseField turns one comma-separated field into a bitmask.
func parseField(field string, f fieldSpec) (uint64, error) {
	var bits uint64
	for _, term := range strings.Split(field, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			return 0, fmt.Errorf("cron: empty term in %s field", f.name)
		}
		termBits, err := parseTerm(term, f)
		if err != nil {
			return 0, err
		}
		bits |= termBits
	}
	return bits, nil
}

func parseTerm(term string, f fieldSpec) (uint64, error) {
	step := uint(1)

	if slash := strings.IndexByte(term, '/'); slash >= 0 {
		stepStr := term[slash+1:]
		term = term[:slash]
		n, err := strconv.ParseUint(stepStr, 10, 32)
		if err != nil || n == 0 {
			return 0, fmt.Errorf("cron: invalid step %q in %s field", stepStr, f.name)
		}
		step = uint(n)
	}

	var low, high uint
	switch {
	case term == "*":
		low, high = f.min, f.max

	case strings.ContainsRune(term, '-'):
		parts := strings.SplitN(term, "-", 2)
		var err error
		if low, err = parseValue(parts[0], f); err != nil {
			return 0, err
		}
		if high, err = parseValue(parts[1], f); err != nil {
			return 0, err
		}
		if low > high {
			return 0, fmt.Errorf("cron: range %q is inverted in %s field", term, f.name)
		}

	default:
		v, err := parseValue(term, f)
		if err != nil {
			return 0, err
		}
		low, high = v, v
		// A bare value with a step means "from here to the end of the range",
		// which is how `*/15` and `5/15` differ.
		if step > 1 {
			high = f.max
		}
	}

	var bits uint64
	for v := low; v <= high; v += step {
		bits |= 1 << v
	}
	return bits, nil
}

func parseValue(s string, f fieldSpec) (uint, error) {
	s = strings.TrimSpace(s)

	if f.names != nil {
		if v, ok := f.names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}

	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("cron: invalid value %q in %s field", s, f.name)
	}
	v := uint(n)

	// Sunday is conventionally both 0 and 7.
	if f.name == "day of week" && v == 7 {
		return 0, nil
	}

	if v < f.min || v > f.max {
		return 0, fmt.Errorf("cron: value %d out of range %d-%d in %s field", v, f.min, f.max, f.name)
	}
	return v, nil
}

// Next returns the first time strictly after t at which the schedule fires, or
// the zero time if it never will within the search limit.
//
// Computed in t's own location. Around a daylight-saving transition a wall
// clock can skip or repeat an hour, so a spec like "0 2 * * *" may fire twice
// or not at all on those two days a year. Running schedulers in UTC avoids the
// question entirely and is what the README recommends.
func (s *Schedule) Next(t time.Time) time.Time {
	if s.every > 0 {
		return t.Add(s.every)
	}

	// Cron has minute granularity, so start from the next whole minute.
	t = t.Truncate(time.Minute).Add(time.Minute)
	limit := t.Add(searchLimit)

	for t.Before(limit) {
		if !bitSet(s.month, uint(t.Month())) {
			// Jump to the first instant of the next month.
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, 1, 0)
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
			continue
		}
		if !bitSet(s.hour, uint(t.Hour())) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Add(time.Hour)
			continue
		}
		if !bitSet(s.minute, uint(t.Minute())) {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}

	return time.Time{}
}

// dayMatches implements the day-of-month / day-of-week rule.
//
// This is the classic cron surprise: when *both* day fields are restricted they
// are OR-ed, not AND-ed. "0 0 1 * MON" fires on the first of the month AND on
// every Monday, not on Mondays that happen to be the first. When only one is
// restricted, the other is ignored.
func (s *Schedule) dayMatches(t time.Time) bool {
	domOK := bitSet(s.dom, uint(t.Day()))
	dowOK := bitSet(s.dow, uint(t.Weekday()))

	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowOK
	case s.dowStar:
		return domOK
	default:
		return domOK || dowOK
	}
}

func bitSet(mask uint64, v uint) bool {
	if v > 63 {
		return false
	}
	return mask&(1<<v) != 0
}
