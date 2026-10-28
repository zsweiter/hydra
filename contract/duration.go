package contract

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ParseDuration reads an ISO 8601 duration with an optional
// sign: "PT30S", "PT1H30M", "P1DT12H", "P2W", "-PT5M", "PT0.5S".
// Years and months are rejected: their length varies, so they
// cannot be an exact duration. A day is 24 hours.
func ParseDuration(s string) (time.Duration, error) {
	orig := s
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	s = strings.ToUpper(s)
	if !strings.HasPrefix(s, "P") || len(s) == 1 {
		return 0, fmt.Errorf("%q is not an ISO 8601 duration (e.g. PT30S, -PT5M, P1D)", orig)
	}
	s = s[1:]

	date, clock, hasT := strings.Cut(s, "T")
	if hasT && clock == "" {
		return 0, fmt.Errorf("%q: no time after T", orig)
	}

	var total float64
	add := func(part, units string, unit map[byte]time.Duration) error {
		last := -1
		for part != "" {
			i := strings.IndexAny(part, units)
			if i <= 0 {
				return fmt.Errorf("%q: expected a number before a unit in %q", orig, part)
			}
			pos := strings.IndexByte(units, part[i])
			if pos <= last {
				return fmt.Errorf("%q: units out of order", orig)
			}
			last = pos
			num := strings.Replace(part[:i], ",", ".", 1)
			if strings.Contains(num, ".") && i+1 != len(part) {
				return fmt.Errorf("%q: only the last unit may have a fraction", orig)
			}
			n, err := strconv.ParseFloat(num, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("%q: bad number %q", orig, part[:i])
			}
			total += n * float64(unit[part[i]])
			part = part[i+1:]
		}
		return nil
	}

	if strings.ContainsAny(date, "YM") {
		return 0, fmt.Errorf("%q: years and months are not exact durations; use days (D) or weeks (W)", orig)
	}
	if err := add(date, "WD", map[byte]time.Duration{'W': 7 * 24 * time.Hour, 'D': 24 * time.Hour}); err != nil {
		return 0, err
	}
	if err := add(clock, "HMS", map[byte]time.Duration{'H': time.Hour, 'M': time.Minute, 'S': time.Second}); err != nil {
		return 0, err
	}

	if total > math.MaxInt64 {
		return 0, errors.New("duration too long")
	}
	d := time.Duration(math.Round(total))
	if neg {
		d = -d
	}
	return d, nil
}

// FormatDuration writes d as an ISO 8601 duration, e.g.
// "PT1H30M", "-PT5M", "P1DT12H", "PT0.5S".
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "PT0S"
	}
	var b strings.Builder
	if d < 0 {
		b.WriteByte('-')
		d = -d
	}
	b.WriteByte('P')

	if days := d / (24 * time.Hour); days > 0 {
		fmt.Fprintf(&b, "%dD", days)
		d -= days * 24 * time.Hour
	}
	if d == 0 {
		return b.String()
	}
	b.WriteByte('T')
	if h := d / time.Hour; h > 0 {
		fmt.Fprintf(&b, "%dH", h)
		d -= h * time.Hour
	}
	if m := d / time.Minute; m > 0 {
		fmt.Fprintf(&b, "%dM", m)
		d -= m * time.Minute
	}
	if d > 0 {
		b.WriteString(strconv.FormatFloat(d.Seconds(), 'f', -1, 64))
		b.WriteByte('S')
	}

	return b.String()
}
