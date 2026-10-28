package contract

import (
	"errors"
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"PT30S":      30 * time.Second,
		"PT1H30M":    90 * time.Minute,
		"P1DT12H":    36 * time.Hour,
		"P2W":        14 * 24 * time.Hour,
		"-PT5M":      -5 * time.Minute,
		"+PT1M":      time.Minute,
		"PT0.5S":     500 * time.Millisecond,
		"PT1,25S":    1250 * time.Millisecond,
		"pt10m":      10 * time.Minute,
		"P1D":        24 * time.Hour,
		"PT0S":       0,
		"PT1M0.001S": time.Minute + time.Millisecond,
	}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "P", "PT", "30s", "P1M", "P1Y", "PT1.5M30S", "PTH", "PT-5M", "PT5S1M", "T5M"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("ParseDuration(%q) accepted", bad)
		}
	}

	for _, d := range []time.Duration{0, time.Second, 90 * time.Minute, 36 * time.Hour, -5 * time.Minute, 1500 * time.Millisecond, 48 * time.Hour} {
		s := FormatDuration(d)
		back, err := ParseDuration(s)
		if err != nil || back != d {
			t.Errorf("FormatDuration(%v) = %q, parsed back as %v, %v", d, s, back, err)
		}
	}
	if got := FormatDuration(90 * time.Minute); got != "PT1H30M" {
		t.Errorf("FormatDuration(90m) = %q", got)
	}
}

func attrs(t *testing.T, e Event, d Definition) map[string]string {
	t.Helper()
	h, err := Headers(NATS, e, d)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for k, v := range h {
		if name, ok := NATS.Attribute(k); ok {
			out[name] = v
		}
	}
	return out
}

var event = Event{ID: "evt-1", Source: "/billing", Type: "com.acme.charge.due", ContentType: "application/json"}

func TestRoundTrip(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	in := Definition{
		Action: ActionReplace, ID: "sub-9", Type: TypeCron, Rule: "0 9 * * *",
		Offset: -5 * time.Minute, Jitter: 30 * time.Second, Start: &start, Count: 12,
		Timezone: "America/Santiago", Misfire: MisfireCatchUp, Priority: -1, Target: "billing.charge.run",
	}
	a := attrs(t, event, in)
	if a["recurrenceoffset"] != "-PT5M" || a["recurrencejitter"] != "PT30S" || a[AttrDataContentType] != "application/json" {
		t.Fatalf("attributes %v", a)
	}
	out, err := ParseDefinition(a)
	if err != nil {
		t.Fatal(err)
	}
	if out.Action != in.Action || out.ID != in.ID || out.Rule != in.Rule || out.Offset != in.Offset ||
		out.Jitter != in.Jitter || !out.Start.Equal(start) || out.Count != 12 || out.Timezone != in.Timezone ||
		out.Misfire != in.Misfire || out.Priority != -1 || out.Target != in.Target {
		t.Fatalf("round trip: %+v", out)
	}
}

func TestHeaderNames(t *testing.T) {
	h, err := Headers(Kafka, event, Definition{ID: "x", Type: TypeOnce, Rule: "PT10M"})
	if err != nil {
		t.Fatal(err)
	}
	if h["ce_recurrenceid"] != "x" || h["ce_specversion"] != "1.0" || h["content-type"] != "application/json" {
		t.Fatalf("kafka headers %v", h)
	}
	if name, ok := NATS.Attribute("Ce-RecurrenceType"); !ok || name != "recurrencetype" {
		t.Fatalf("header names must match without case: %q %v", name, ok)
	}
}

func TestParseDefinitionRejects(t *testing.T) {
	base := func() map[string]string {
		return attrs(t, event, Definition{ID: "x", Type: TypeInterval, Rule: "PT1M"})
	}
	cases := map[string]func(map[string]string){
		"no id":        func(a map[string]string) { delete(a, AttrRecID) },
		"no source":    func(a map[string]string) { delete(a, AttrSource) },
		"old spec":     func(a map[string]string) { a[AttrSpecVersion] = "0.3" },
		"typo":         func(a map[string]string) { a["recurrencerul"] = "x" },
		"bad type":     func(a map[string]string) { a[AttrRecType] = "weekly" },
		"bad action":   func(a map[string]string) { a[AttrAction] = "stop" },
		"go duration":  func(a map[string]string) { a[AttrOffset] = "-5m" },
		"months":       func(a map[string]string) { a[AttrJitter] = "P1M" },
		"bad count":    func(a map[string]string) { a[AttrCount] = "many" },
		"bad start":    func(a map[string]string) { a[AttrStart] = "tomorrow" },
		"hash in id":   func(a map[string]string) { a[AttrRecID] = "a#1" },
		"structured":   func(a map[string]string) { a[AttrDataContentType] = StructuredContentType + "; charset=utf-8" },
		"bad misfire":  func(a map[string]string) { a[AttrMisfire] = "later" },
		"missing rule": func(a map[string]string) { delete(a, AttrRule) },
	}
	for name, mutate := range cases {
		a := base()
		mutate(a)
		if _, err := ParseDefinition(a); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	// A state change needs only the id.
	a := attrs(t, event, Definition{Action: ActionPause, ID: "x"})
	if d, err := ParseDefinition(a); err != nil || !d.ChangesState() {
		t.Fatalf("pause: %+v %v", d, err)
	}
}

func TestParseOccurrence(t *testing.T) {
	if _, ok, _ := ParseOccurrence(map[string]string{AttrSpecVersion: "1.0", AttrType: "x"}); ok {
		t.Fatal("a plain event parsed as an occurrence")
	}
	occ := map[string]string{
		AttrSchedule:   "sub-9",
		AttrOccurrence: "2026-11-01T12:00:00Z",
		AttrRevision:   "2",
		AttrTrace:      "t1",
	}
	o, ok, err := ParseOccurrence(occ)
	if !ok || err != nil || o.Schedule != "sub-9" || o.Revision != 2 || o.Trace != "t1" || !o.At.Equal(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("occurrence %+v %v %v", o, ok, err)
	}
}
