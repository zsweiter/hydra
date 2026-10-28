// Package contract is the wire contract of hydra-scheduler:
// recurrence definitions and their occurrences as CloudEvents
// 1.0 in binary content mode.
//
// Every attribute travels in a header and the body is the
// event's data, which hydra stores and republishes byte for
// byte without reading it. The recurrence goes in extension
// attributes (recurrence*); occurrences carry hydra* extensions.
//
// Producers build definitions with Definition and Event;
// consumers recognise occurrences with ParseOccurrence. The
// scheduler parses definitions with ParseDefinition. The
// package has no dependencies, so any producer can use it.
//
// See docs/CONTRACT.md for the full contract.
package contract

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SpecVersion is the CloudEvents version of the contract.
const SpecVersion = "1.0"

// Binding is how a broker carries CloudEvents attributes in
// headers: a prefix per attribute name, except datacontenttype,
// which is the content-type header.
type Binding struct {
	Prefix string
}

// Bindings of the CloudEvents protocol specifications.
var (
	NATS  = Binding{Prefix: "ce-"}
	Kafka = Binding{Prefix: "ce_"}
)

// ContentTypeHeader carries datacontenttype in every binding.
const ContentTypeHeader = "content-type"

// StructuredContentType marks a structured-mode CloudEvent,
// which the contract does not accept.
const StructuredContentType = "application/cloudevents+json"

// Header returns the header name of attribute.
func (b Binding) Header(attribute string) string {
	if attribute == AttrDataContentType {
		return ContentTypeHeader
	}

	return b.Prefix + attribute
}

// Attribute returns the attribute a header carries, if it is a
// CloudEvents header of this binding. Header names are compared
// without case.
func (b Binding) Attribute(header string) (string, bool) {
	h := strings.ToLower(header)
	if h == ContentTypeHeader {
		return AttrDataContentType, true
	}

	if name, ok := strings.CutPrefix(h, b.Prefix); ok && name != "" {
		return name, true
	}

	return "", false
}

// Core CloudEvents attributes.
const (
	AttrSpecVersion     = "specversion"
	AttrID              = "id"
	AttrSource          = "source"
	AttrType            = "type"
	AttrSubject         = "subject"
	AttrTime            = "time"
	AttrDataContentType = "datacontenttype"
	AttrDataSchema      = "dataschema"

	// AttrTraceParent and AttrTraceState are the CloudEvents
	// distributed tracing extension (W3C Trace Context).
	AttrTraceParent = "traceparent"
	AttrTraceState  = "tracestate"
)

// Recurrence extension attributes, set by producers on
// definitions. Names follow the CloudEvents rules: lowercase
// letters and digits, at most 20 characters.
const (
	AttrAction   = "recurrenceaction"   // create (default), replace, pause, resume, cancel, delete
	AttrRecID    = "recurrenceid"       // required: the producer's stable id of the schedule
	AttrRecType  = "recurrencetype"     // cron, rrule, interval, once
	AttrRule     = "recurrencerule"     // expression, ISO 8601 interval, or once: RFC 3339 time or ISO 8601 delay
	AttrOffset   = "recurrenceoffset"   // ISO 8601 duration, may be negative: -PT5M fires five minutes early
	AttrJitter   = "recurrencejitter"   // ISO 8601 duration: spread over [0, jitter), fixed per id
	AttrStart    = "recurrencestart"    // RFC 3339
	AttrEnd      = "recurrenceend"      // RFC 3339
	AttrCount    = "recurrencecount"    // maximum occurrences, 0 = unlimited
	AttrTimezone = "recurrencetimezone" // IANA zone, default UTC
	AttrMisfire  = "recurrencemisfire"  // skip, run_once (default), catch_up
	AttrPriority = "recurrencepriority" // integer, lower first
	AttrTarget   = "recurrencetarget"   // subject or topic of the occurrences, default the definition's
)

// RecurrencePrefix starts every recurrence attribute.
const RecurrencePrefix = "recurrence"

// DefinitionPrefix is the default prefix of the subjects (or
// topics) definitions are published on. A definition on
// "schedule.notification.billing" schedules occurrences on
// "notification.billing", so the consumers of that subject only
// ever receive occurrences, and definitions can live in a
// stream of their own.
const DefinitionPrefix = "schedule."

// DefinitionSubject returns the subject to publish the
// definitions of occurrences due on subject, under the default
// prefix.
func DefinitionSubject(subject string) string {
	return DefinitionPrefix + subject
}

var recurrenceAttrs = map[string]bool{
	AttrAction: true, AttrRecID: true, AttrRecType: true, AttrRule: true, AttrOffset: true,
	AttrJitter: true, AttrStart: true, AttrEnd: true, AttrCount: true, AttrTimezone: true,
	AttrMisfire: true, AttrPriority: true, AttrTarget: true,
}

// Hydra extension attributes, set by the scheduler on
// occurrences.
const (
	AttrSchedule   = "hydraschedule"   // recurrenceid of the definition
	AttrOccurrence = "hydraoccurrence" // nominal time, RFC 3339, without offset or jitter
	AttrRevision   = "hydrarevision"   // revision of the definition that produced it
	AttrTrace      = "hydratrace"      // trace id of the change that made that revision
)

// Actions of AttrAction.
const (
	ActionCreate  = "create"
	ActionReplace = "replace"
	ActionPause   = "pause"
	ActionResume  = "resume"
	ActionCancel  = "cancel"
	ActionDelete  = "delete"
)

// Recurrence types of AttrRecType.
const (
	TypeCron     = "cron"
	TypeRRule    = "rrule"
	TypeInterval = "interval"
	TypeOnce     = "once"
)

// Misfire policies of AttrMisfire.
const (
	MisfireSkip    = "skip"
	MisfireRunOnce = "run_once"
	MisfireCatchUp = "catch_up"
)

// ErrInvalid reports a definition that breaks the contract.
var ErrInvalid = errors.New("invalid recurrence definition")

// Event holds the core attributes of a CloudEvent.
type Event struct {
	ID          string // required, unique per event
	Source      string // required
	Type        string // required
	Subject     string
	Time        time.Time
	ContentType string // the data's media type
	DataSchema  string
	TraceParent string // W3C traceparent, for the trace of the change
	TraceState  string
}

// Attributes returns the event's attributes.
func (e Event) Attributes() map[string]string {
	attrs := map[string]string{
		AttrSpecVersion: SpecVersion,
		AttrID:          e.ID,
		AttrSource:      e.Source,
		AttrType:        e.Type,
	}

	set := func(name, value string) {
		if value != "" {
			attrs[name] = value
		}
	}

	set(AttrSubject, e.Subject)
	set(AttrDataContentType, e.ContentType)
	set(AttrDataSchema, e.DataSchema)
	set(AttrTraceParent, e.TraceParent)
	set(AttrTraceState, e.TraceState)
	if !e.Time.IsZero() {
		attrs[AttrTime] = e.Time.UTC().Format(time.RFC3339Nano)
	}

	return attrs
}

// Definition is a recurrence definition, or a change of its
// state (pause, resume, cancel, delete), carried by an event.
type Definition struct {
	Action string // default ActionCreate
	ID     string // required

	// For create and replace.
	Type     string
	Rule     string
	Offset   time.Duration
	Jitter   time.Duration
	Start    *time.Time
	End      *time.Time
	Count    int
	Timezone string
	Misfire  string
	Priority int
	Target   string
}

// ChangesState reports whether the definition only changes the
// state of an existing schedule.
func (d *Definition) ChangesState() bool {
	switch d.Action {
	case ActionPause, ActionResume, ActionCancel, ActionDelete:
		return true
	}

	return false
}

// Attributes returns the recurrence attributes of d, after
// checking what can be checked without parsing the rule.
func (d Definition) Attributes() (map[string]string, error) {
	if d.Action == "" {
		d.Action = ActionCreate
	}
	if err := d.validate(); err != nil {
		return nil, err
	}

	attrs := map[string]string{AttrRecID: d.ID}
	if d.Action != ActionCreate {
		attrs[AttrAction] = d.Action
	}
	if d.ChangesState() {
		return attrs, nil
	}

	attrs[AttrRecType] = d.Type
	set := func(name, value string) {
		if value != "" {
			attrs[name] = value
		}
	}
	set(AttrRule, d.Rule)
	set(AttrTimezone, d.Timezone)
	set(AttrMisfire, d.Misfire)
	set(AttrTarget, d.Target)
	if d.Offset != 0 {
		attrs[AttrOffset] = FormatDuration(d.Offset)
	}
	if d.Jitter != 0 {
		attrs[AttrJitter] = FormatDuration(d.Jitter)
	}
	if d.Start != nil {
		attrs[AttrStart] = d.Start.UTC().Format(time.RFC3339Nano)
	}
	if d.End != nil {
		attrs[AttrEnd] = d.End.UTC().Format(time.RFC3339Nano)
	}
	if d.Count != 0 {
		attrs[AttrCount] = strconv.Itoa(d.Count)
	}
	if d.Priority != 0 {
		attrs[AttrPriority] = strconv.Itoa(d.Priority)
	}

	return attrs, nil
}

func (d *Definition) validate() error {
	switch d.Action {
	case ActionCreate, ActionReplace, ActionPause, ActionResume, ActionCancel, ActionDelete:
	default:
		return fmt.Errorf("%w: unknown %s %q", ErrInvalid, AttrAction, d.Action)
	}
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalid, AttrRecID)
	}
	// "#" separates a schedule from its archived revisions.
	if strings.Contains(d.ID, "#") {
		return fmt.Errorf("%w: %s must not contain '#'", ErrInvalid, AttrRecID)
	}
	if d.ChangesState() {
		return nil
	}

	switch d.Type {
	case TypeCron, TypeRRule, TypeInterval:
		if d.Rule == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalid, AttrRule)
		}
	case TypeOnce:
	default:
		return fmt.Errorf("%w: unknown %s %q", ErrInvalid, AttrRecType, d.Type)
	}
	switch d.Misfire {
	case "", MisfireSkip, MisfireRunOnce, MisfireCatchUp:
	default:
		return fmt.Errorf("%w: unknown %s %q", ErrInvalid, AttrMisfire, d.Misfire)
	}
	if d.Count < 0 {
		return fmt.Errorf("%w: %s must not be negative", ErrInvalid, AttrCount)
	}
	if d.Jitter < 0 {
		return fmt.Errorf("%w: %s must not be negative", ErrInvalid, AttrJitter)
	}
	if d.Start != nil && d.End != nil && d.End.Before(*d.Start) {
		return fmt.Errorf("%w: %s is before %s", ErrInvalid, AttrEnd, AttrStart)
	}
	return nil
}

// Headers returns the headers of a definition event in binding:
// the event's attributes and the definition's.
func Headers(b Binding, e Event, d Definition) (map[string]string, error) {
	if e.ID == "" || e.Source == "" || e.Type == "" {
		return nil, fmt.Errorf("%w: id, source and type are required", ErrInvalid)
	}

	rec, err := d.Attributes()
	if err != nil {
		return nil, err
	}

	headers := make(map[string]string, len(rec)+8)
	for name, value := range e.Attributes() {
		headers[b.Header(name)] = value
	}

	for name, value := range rec {
		headers[b.Header(name)] = value
	}

	return headers, nil
}

// ParseDefinition reads the definition carried by attrs, the
// CloudEvents attributes of a message by name (see
// Binding.Attribute). Errors wrap ErrInvalid.
func ParseDefinition(attrs map[string]string) (*Definition, error) {
	if v := attrs[AttrSpecVersion]; v != SpecVersion {
		return nil, fmt.Errorf("%w: %s %q, want %s", ErrInvalid, AttrSpecVersion, v, SpecVersion)
	}
	if strings.HasPrefix(strings.ToLower(attrs[AttrDataContentType]), StructuredContentType) {
		return nil, fmt.Errorf("%w: structured mode is not supported, send a binary-mode CloudEvent", ErrInvalid)
	}
	for _, name := range []string{AttrID, AttrSource, AttrType} {
		if attrs[name] == "" {
			return nil, fmt.Errorf("%w: %s is required", ErrInvalid, name)
		}
	}
	for name := range attrs {
		if strings.HasPrefix(name, RecurrencePrefix) && !recurrenceAttrs[name] {
			return nil, fmt.Errorf("%w: unknown attribute %q", ErrInvalid, name)
		}
	}

	d := &Definition{
		Action:   strings.ToLower(strings.TrimSpace(attrs[AttrAction])),
		ID:       strings.TrimSpace(attrs[AttrRecID]),
		Type:     strings.ToLower(strings.TrimSpace(attrs[AttrRecType])),
		Rule:     strings.TrimSpace(attrs[AttrRule]),
		Timezone: strings.TrimSpace(attrs[AttrTimezone]),
		Misfire:  strings.ToLower(strings.TrimSpace(attrs[AttrMisfire])),
		Target:   strings.TrimSpace(attrs[AttrTarget]),
	}
	if d.Action == "" {
		d.Action = ActionCreate
	}

	var err error
	if d.Offset, err = optDuration(attrs, AttrOffset); err != nil {
		return nil, err
	}
	if d.Jitter, err = optDuration(attrs, AttrJitter); err != nil {
		return nil, err
	}
	if d.Start, err = optTime(attrs, AttrStart); err != nil {
		return nil, err
	}
	if d.End, err = optTime(attrs, AttrEnd); err != nil {
		return nil, err
	}
	if d.Count, err = optInt(attrs, AttrCount); err != nil {
		return nil, err
	}
	if d.Priority, err = optInt(attrs, AttrPriority); err != nil {
		return nil, err
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	return d, nil
}

func optDuration(attrs map[string]string, name string) (time.Duration, error) {
	v := strings.TrimSpace(attrs[name])
	if v == "" {
		return 0, nil
	}
	d, err := ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: %v", ErrInvalid, name, err)
	}
	return d, nil
}

func optTime(attrs map[string]string, name string) (*time.Time, error) {
	v := strings.TrimSpace(attrs[name])
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be RFC 3339: %v", ErrInvalid, name, err)
	}
	return &t, nil
}

func optInt(attrs map[string]string, name string) (int, error) {
	v := strings.TrimSpace(attrs[name])
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer", ErrInvalid, name)
	}
	return n, nil
}

// Occurrence is what an occurrence's hydra attributes say.
type Occurrence struct {
	Schedule string    // recurrenceid of the definition
	At       time.Time // nominal time, without offset or jitter
	Revision int
	Trace    string
}

// ParseOccurrence reads the hydra attributes of an occurrence.
// ok is false when attrs are not one: any other event on the
// subject.
func ParseOccurrence(attrs map[string]string) (o Occurrence, ok bool, err error) {
	if _, isOccurrence := attrs[AttrSchedule]; !isOccurrence {
		return o, false, nil
	}
	o.Schedule = attrs[AttrSchedule]
	o.Trace = attrs[AttrTrace]
	if o.At, err = time.Parse(time.RFC3339Nano, attrs[AttrOccurrence]); err != nil {
		return o, true, fmt.Errorf("%s: %w", AttrOccurrence, err)
	}

	if v := attrs[AttrRevision]; v != "" {
		if o.Revision, err = strconv.Atoi(v); err != nil {
			return o, true, fmt.Errorf("%s: %w", AttrRevision, err)
		}
	}

	return o, true, nil
}
