package parsers

import (
	"fmt"
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
