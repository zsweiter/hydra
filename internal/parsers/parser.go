package parsers

import (
	"errors"
	"time"
)

type Schedule interface {
	Next(time.Time) time.Time
}

type RecurrenceType uint8

const (
	RecurrenceCron RecurrenceType = iota
	RecurrenceRrule
)

type ParseSchedule struct {
	cronParser  *CronParser
	rruleParser *RRuleParser
}

func NewScheduleParser() ParseSchedule {
	return ParseSchedule{
		cronParser: NewCronParser(
			Minute | Hour | Dom | Month | Dow | Descriptor,
		),
		rruleParser: NewRRuleParser(),
	}
}

func (p ParseSchedule) Parse(recurrenceType RecurrenceType, pattern string) (Schedule, error) {
	switch recurrenceType {
	case RecurrenceCron:
		return p.cronParser.Parse(pattern)
	case RecurrenceRrule:
		return p.rruleParser.Parse(pattern)
	default:
		return nil, errors.New("unsupported recurrence type")
	}
}
