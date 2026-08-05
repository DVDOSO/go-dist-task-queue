package cron

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, spec string) *Schedule {
	t.Helper()
	s, err := Parse(spec)
	if err != nil {
		t.Fatalf("Parse(%q): %v", spec, err)
	}
	return s
}

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseRejectsBadSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
	}{
		{"empty", ""},
		{"blank", "   "},
		{"too few fields", "* * * *"},
		{"too many fields", "* * * * * *"},
		{"minute out of range", "60 * * * *"},
		{"hour out of range", "* 24 * * *"},
		{"day of month zero", "* * 0 * *"},
		{"day of month too high", "* * 32 * *"},
		{"month zero", "* * * 0 *"},
		{"month too high", "* * * 13 *"},
		{"day of week too high", "* * * * 8"},
		{"inverted range", "30-10 * * * *"},
		{"zero step", "*/0 * * * *"},
		{"non-numeric step", "*/x * * * *"},
		{"garbage value", "abc * * * *"},
		{"unknown month name", "* * * FOO *"},
		{"empty list term", "1,,2 * * * *"},
		{"negative @every", "@every -5m"},
		{"zero @every", "@every 0s"},
		{"unparseable @every", "@every banana"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Parse(tt.spec); err == nil {
				t.Errorf("Parse(%q) succeeded, want an error", tt.spec)
			}
		})
	}
}

func TestNext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
		from string
		want string
	}{
		{"every minute", "* * * * *", "2026-03-10 12:00:30", "2026-03-10 12:01:00"},
		{"strictly after, never equal", "* * * * *", "2026-03-10 12:00:00", "2026-03-10 12:01:00"},
		{"specific minute", "30 * * * *", "2026-03-10 12:00:00", "2026-03-10 12:30:00"},
		{"specific minute rolls the hour", "30 * * * *", "2026-03-10 12:45:00", "2026-03-10 13:30:00"},
		{"daily at a time", "0 9 * * *", "2026-03-10 12:00:00", "2026-03-11 09:00:00"},
		{"daily before the time", "0 9 * * *", "2026-03-10 08:00:00", "2026-03-10 09:00:00"},

		{"step over the full range", "*/15 * * * *", "2026-03-10 12:00:00", "2026-03-10 12:15:00"},
		{"step wraps the hour", "*/15 * * * *", "2026-03-10 12:50:00", "2026-03-10 13:00:00"},
		{"step from an offset", "5/20 * * * *", "2026-03-10 12:00:00", "2026-03-10 12:05:00"},
		{"step from an offset continues", "5/20 * * * *", "2026-03-10 12:10:00", "2026-03-10 12:25:00"},
		{"stepped range", "0-30/10 * * * *", "2026-03-10 12:12:00", "2026-03-10 12:20:00"},
		{"stepped range does not exceed its high", "0-30/10 * * * *", "2026-03-10 12:35:00", "2026-03-10 13:00:00"},

		{"list of minutes", "0,15,45 * * * *", "2026-03-10 12:20:00", "2026-03-10 12:45:00"},
		{"list wraps", "0,15,45 * * * *", "2026-03-10 12:50:00", "2026-03-10 13:00:00"},
		{"range of hours", "0 9-17 * * *", "2026-03-10 18:30:00", "2026-03-11 09:00:00"},

		{"day of month", "0 0 15 * *", "2026-03-10 00:00:00", "2026-03-15 00:00:00"},
		{"day of month rolls to next month", "0 0 15 * *", "2026-03-20 00:00:00", "2026-04-15 00:00:00"},
		{"month name", "0 0 1 JUL *", "2026-03-10 00:00:00", "2026-07-01 00:00:00"},
		{"month number", "0 0 1 7 *", "2026-03-10 00:00:00", "2026-07-01 00:00:00"},
		{"month rolls to next year", "0 0 1 1 *", "2026-03-10 00:00:00", "2027-01-01 00:00:00"},

		// 2026-03-10 is a Tuesday.
		{"day of week by name", "0 0 * * FRI", "2026-03-10 00:00:00", "2026-03-13 00:00:00"},
		{"day of week by number", "0 0 * * 5", "2026-03-10 00:00:00", "2026-03-13 00:00:00"},
		{"sunday as 0", "0 0 * * 0", "2026-03-10 00:00:00", "2026-03-15 00:00:00"},
		{"sunday as 7", "0 0 * * 7", "2026-03-10 00:00:00", "2026-03-15 00:00:00"},

		{"leap day exists in 2028", "0 0 29 2 *", "2026-03-10 00:00:00", "2028-02-29 00:00:00"},

		{"descriptor hourly", "@hourly", "2026-03-10 12:30:00", "2026-03-10 13:00:00"},
		{"descriptor daily", "@daily", "2026-03-10 12:30:00", "2026-03-11 00:00:00"},
		{"descriptor midnight", "@midnight", "2026-03-10 12:30:00", "2026-03-11 00:00:00"},
		{"descriptor weekly", "@weekly", "2026-03-10 12:30:00", "2026-03-15 00:00:00"},
		{"descriptor monthly", "@monthly", "2026-03-10 12:30:00", "2026-04-01 00:00:00"},
		{"descriptor yearly", "@yearly", "2026-03-10 12:30:00", "2027-01-01 00:00:00"},
		{"descriptor is case insensitive", "@DAILY", "2026-03-10 12:30:00", "2026-03-11 00:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := mustParse(t, tt.spec).Next(at(tt.from))
			want := at(tt.want)
			if !got.Equal(want) {
				t.Errorf("Parse(%q).Next(%s) = %s, want %s", tt.spec, tt.from, got, want)
			}
		})
	}
}

// TestDayOfMonthAndDayOfWeekAreOred pins down the classic cron surprise: when
// both day fields are restricted they are OR-ed, not AND-ed.
func TestDayOfMonthAndDayOfWeekAreOred(t *testing.T) {
	t.Parallel()

	// 1st of the month OR any Monday. March 2026: the 1st is a Sunday, and the
	// Mondays are the 2nd, 9th, 16th...
	s := mustParse(t, "0 0 1 * MON")

	tests := []struct {
		from string
		want string
	}{
		{"2026-02-28 12:00:00", "2026-03-01 00:00:00"}, // the 1st, though a Sunday
		{"2026-03-01 12:00:00", "2026-03-02 00:00:00"}, // the next Monday
		{"2026-03-02 12:00:00", "2026-03-09 00:00:00"}, // the Monday after
	}

	for _, tt := range tests {
		t.Run(tt.from, func(t *testing.T) {
			t.Parallel()
			got := s.Next(at(tt.from))
			if want := at(tt.want); !got.Equal(want) {
				t.Errorf("Next(%s) = %s, want %s", tt.from, got, want)
			}
		})
	}
}

// TestOnlyOneDayFieldRestricted: with one day field left as *, the other alone
// decides, rather than the OR rule making every day match.
func TestOnlyOneDayFieldRestricted(t *testing.T) {
	t.Parallel()

	t.Run("day of week only", func(t *testing.T) {
		t.Parallel()
		// Fridays only. 2026-03-10 is a Tuesday.
		got := mustParse(t, "0 0 * * FRI").Next(at("2026-03-10 00:00:00"))
		if want := at("2026-03-13 00:00:00"); !got.Equal(want) {
			t.Errorf("Next = %s, want %s", got, want)
		}
	})

	t.Run("day of month only", func(t *testing.T) {
		t.Parallel()
		got := mustParse(t, "0 0 20 * *").Next(at("2026-03-10 00:00:00"))
		if want := at("2026-03-20 00:00:00"); !got.Equal(want) {
			t.Errorf("Next = %s, want %s", got, want)
		}
	})
}

// TestUnsatisfiableSpecTerminates: "30 February" parses but never fires, and
// the search must give up rather than spin forever.
func TestUnsatisfiableSpecTerminates(t *testing.T) {
	t.Parallel()

	got := mustParse(t, "0 0 30 2 *").Next(at("2026-03-10 00:00:00"))
	if !got.IsZero() {
		t.Errorf("Next = %s, want the zero time for an unsatisfiable spec", got)
	}
}

func TestEveryDescriptor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		spec string
		want time.Duration
	}{
		{"@every 30s", 30 * time.Second},
		{"@every 5m", 5 * time.Minute},
		{"@every 1h30m", 90 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			t.Parallel()
			from := at("2026-03-10 12:00:00")
			got := mustParse(t, tt.spec).Next(from)
			if want := from.Add(tt.want); !got.Equal(want) {
				t.Errorf("Next = %s, want %s", got, want)
			}
		})
	}
}

// TestNextIsRepeatable: feeding Next its own output must walk forward, never
// stall or go backwards. A scheduler calls it in exactly this loop.
func TestNextIsRepeatable(t *testing.T) {
	t.Parallel()

	s := mustParse(t, "*/7 3-5 * * *")
	cur := at("2026-03-10 00:00:00")

	for i := range 200 {
		next := s.Next(cur)
		if next.IsZero() {
			t.Fatalf("iteration %d: Next returned the zero time", i)
		}
		if !next.After(cur) {
			t.Fatalf("iteration %d: Next(%s) = %s, which is not later", i, cur, next)
		}
		if h := next.Hour(); h < 3 || h > 5 {
			t.Fatalf("iteration %d: fired at hour %d, outside the 3-5 range", i, h)
		}
		if next.Minute()%7 != 0 {
			t.Fatalf("iteration %d: fired at minute %d, not a multiple of 7", i, next.Minute())
		}
		cur = next
	}
}

func TestNextPreservesLocation(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("America/Toronto")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}

	from := time.Date(2026, 3, 10, 12, 0, 0, 0, loc)
	got := mustParse(t, "0 15 * * *").Next(from)

	if got.Location() != loc {
		t.Errorf("Next returned location %v, want %v", got.Location(), loc)
	}
	if got.Hour() != 15 {
		t.Errorf("Next fired at hour %d in %v, want 15", got.Hour(), loc)
	}
}
