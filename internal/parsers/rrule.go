package parsers

import (
	"fmt"
	"strings"
	"time"

	"github.com/teambition/rrule-go"
)

type RRuleSchedule struct {
	rrule *rrule.RRule
}

func (s RRuleSchedule) Next(t time.Time) time.Time {
	return s.rrule.After(t, false)
}

type RRuleParser struct {
}

func NewRRuleParser() *RRuleParser {
	return &RRuleParser{}
}

func (p RRuleParser) Parse(pattern string) (Schedule, error) {
	if len(pattern) == 0 {
		return nil, fmt.Errorf("empty spec string")
	}

	r, err := rrule.StrToRRule(pattern)
	if err != nil {
		return nil, err
	}

	return RRuleSchedule{rrule: r}, nil
}

// HasDTStart reports whether an RRULE expression carries
// its own DTSTART anchor.
func HasDTStart(pattern string) bool {
	return strings.Contains(strings.ToUpper(pattern), "DTSTART")
}

// WithDTStart anchors the rule at dt. Without an anchor,
// rrule-go uses the parse time, which moves on every restart
// and shifts INTERVAL>1 rules.
func (s RRuleSchedule) WithDTStart(dt time.Time) RRuleSchedule {
	r := *s.rrule
	r.DTStart(dt)
	return RRuleSchedule{rrule: &r}
}
