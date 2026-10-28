//! The hydra-scheduler wire contract (docs/CONTRACT.md), with no
//! dependencies.
//!
//! A producer publishes a [`Definition`] as a CloudEvent in
//! binary mode on [`definition_subject`]: every attribute is a
//! header, and the body is carried untouched. hydra-scheduler
//! publishes each occurrence on the subject without the prefix,
//! where [`occurrence`] reads where it came from.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Header prefix of the CloudEvents NATS binding (Kafka uses `ce_`).
pub const NATS_PREFIX: &str = "ce-";

/// Prefix of the subjects definitions are published on.
pub const DEFINITION_PREFIX: &str = "schedule.";

/// The subject to publish definitions on, for occurrences due on
/// `subject`: `notification.billing` becomes
/// `schedule.notification.billing`.
pub fn definition_subject(subject: &str) -> String {
    format!("{DEFINITION_PREFIX}{subject}")
}

/// What a definition does to its schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Create,
    Replace,
    Pause,
    Resume,
    Cancel,
    Delete,
}

impl Action {
    fn as_str(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Replace => "replace",
            Action::Pause => "pause",
            Action::Resume => "resume",
            Action::Cancel => "cancel",
            Action::Delete => "delete",
        }
    }

    /// Whether the action only changes the state of an existing
    /// schedule, so it carries no recurrence.
    fn is_state_change(self) -> bool {
        matches!(
            self,
            Action::Pause | Action::Resume | Action::Cancel | Action::Delete
        )
    }
}

/// When a schedule fires.
#[derive(Clone, Debug)]
pub enum Recurrence {
    /// A cron expression, e.g. `0 9 * * *`.
    Cron(String),
    /// An RFC 5545 rule, e.g. `FREQ=WEEKLY;BYDAY=MO`.
    RRule(String),
    /// A fixed interval.
    Interval(Duration),
    /// Once: an RFC 3339 time, or an ISO 8601 delay (`PT30S`).
    Once(String),
}

impl Recurrence {
    fn kind(&self) -> &'static str {
        match self {
            Recurrence::Cron(_) => "cron",
            Recurrence::RRule(_) => "rrule",
            Recurrence::Interval(_) => "interval",
            Recurrence::Once(_) => "once",
        }
    }

    fn rule(&self) -> String {
        match self {
            Recurrence::Cron(expr) | Recurrence::RRule(expr) | Recurrence::Once(expr) => {
                expr.clone()
            }
            Recurrence::Interval(every) => iso_duration(every.as_millis() as i64),
        }
    }
}

/// What to do with the occurrences missed while no scheduler ran.
#[derive(Clone, Copy, Debug)]
pub enum Misfire {
    Skip,
    /// The default: fire once, then continue.
    RunOnce,
    CatchUp,
}

impl Misfire {
    fn as_str(self) -> &'static str {
        match self {
            Misfire::Skip => "skip",
            Misfire::RunOnce => "run_once",
            Misfire::CatchUp => "catch_up",
        }
    }
}

/// The CloudEvent carrying a definition.
#[derive(Clone, Debug)]
pub struct Event {
    /// Unique per event. Also send it as `Nats-Msg-Id`, so
    /// JetStream drops a retried publish.
    pub id: String,
    pub source: String,
    pub kind: String,
    /// Media type of the body, which hydra never reads.
    pub content_type: Option<String>,
    /// W3C traceparent: its trace id is recorded with the change.
    pub traceparent: Option<String>,
}

impl Event {
    pub fn new(id: impl Into<String>, source: &str, kind: &str) -> Self {
        Event {
            id: id.into(),
            source: source.into(),
            kind: kind.into(),
            content_type: None,
            traceparent: None,
        }
    }

    pub fn content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn traceparent(mut self, traceparent: impl Into<String>) -> Self {
        self.traceparent = Some(traceparent.into());
        self
    }
}

/// A recurrence definition, or a change of its state.
///
/// ```
/// # use hydra_client_example::contract::{Definition, Recurrence};
/// let standup = Definition::create("standup-u-42", Recurrence::Cron("* * * * *".into()))
///     .offset_ms(-10_000)
///     .timezone("America/Santiago");
/// ```
#[derive(Clone, Debug)]
pub struct Definition {
    id: String,
    action: Action,
    recurrence: Option<Recurrence>,
    offset_ms: i64,
    jitter: Duration,
    start: Option<String>,
    end: Option<String>,
    count: u32,
    timezone: Option<String>,
    misfire: Option<Misfire>,
    priority: i32,
    target: Option<String>,
}

impl Definition {
    /// Creates the schedule `id`, your stable id for it. Sending
    /// the same definition again changes nothing.
    pub fn create(id: &str, recurrence: Recurrence) -> Self {
        Definition {
            id: id.into(),
            action: Action::Create,
            recurrence: Some(recurrence),
            offset_ms: 0,
            jitter: Duration::ZERO,
            start: None,
            end: None,
            count: 0,
            timezone: None,
            misfire: None,
            priority: 0,
            target: None,
        }
    }

    /// Replaces the schedule `id`: the current revision is
    /// archived and a new one starts.
    pub fn replace(id: &str, recurrence: Recurrence) -> Self {
        Definition {
            action: Action::Replace,
            ..Definition::create(id, recurrence)
        }
    }

    /// Pauses, resumes, cancels or deletes the schedule `id`.
    pub fn change(id: &str, action: Action) -> Self {
        Definition {
            action,
            recurrence: None,
            ..Definition::create(id, Recurrence::Once(String::new()))
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn action(&self) -> Action {
        self.action
    }

    /// Shifts every occurrence; negative fires early.
    pub fn offset_ms(mut self, offset_ms: i64) -> Self {
        self.offset_ms = offset_ms;
        self
    }

    /// Spreads occurrences over `[0, jitter)`, with a fixed shift
    /// per schedule, to flatten peaks.
    pub fn jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }

    /// First nominal occurrence, RFC 3339.
    pub fn start(mut self, start: &str) -> Self {
        self.start = Some(start.into());
        self
    }

    /// Last nominal occurrence, RFC 3339.
    pub fn end(mut self, end: &str) -> Self {
        self.end = Some(end.into());
        self
    }

    /// Maximum number of occurrences.
    pub fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    /// IANA zone of the rule; UTC by default.
    pub fn timezone(mut self, timezone: &str) -> Self {
        self.timezone = Some(timezone.into());
        self
    }

    pub fn misfire(mut self, misfire: Misfire) -> Self {
        self.misfire = Some(misfire);
        self
    }

    /// Lower fires first when several are due.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Subject of the occurrences, instead of the definition's
    /// subject without the prefix.
    pub fn target(mut self, target: &str) -> Self {
        self.target = Some(target.into());
        self
    }

    /// The headers of the definition event, with `prefix`
    /// ([`NATS_PREFIX`] for NATS).
    pub fn headers(&self, event: &Event, prefix: &str) -> Result<Vec<(String, String)>, Error> {
        if self.id.trim().is_empty() || self.id.contains('#') {
            return Err(Error::InvalidId);
        }
        if event.id.is_empty() || event.source.is_empty() || event.kind.is_empty() {
            return Err(Error::IncompleteEvent);
        }

        let mut headers = Headers {
            prefix,
            list: Vec::new(),
        };
        headers.attr("specversion", "1.0");
        headers.attr("id", &event.id);
        headers.attr("source", &event.source);
        headers.attr("type", &event.kind);
        headers.attr("recurrenceid", &self.id);
        if let Some(content_type) = &event.content_type {
            headers
                .list
                .push(("content-type".into(), content_type.clone()));
        }
        if let Some(traceparent) = &event.traceparent {
            headers.attr("traceparent", traceparent);
        }
        if self.action != Action::Create {
            headers.attr("recurrenceaction", self.action.as_str());
        }
        if self.action.is_state_change() {
            return Ok(headers.list);
        }

        let recurrence = self.recurrence.as_ref().ok_or(Error::MissingRecurrence)?;
        headers.attr("recurrencetype", recurrence.kind());
        headers.attr("recurrencerule", &recurrence.rule());

        if self.offset_ms != 0 {
            headers.attr("recurrenceoffset", &iso_duration(self.offset_ms));
        }
        if !self.jitter.is_zero() {
            headers.attr(
                "recurrencejitter",
                &iso_duration(self.jitter.as_millis() as i64),
            );
        }
        if self.count != 0 {
            headers.attr("recurrencecount", &self.count.to_string());
        }
        if self.priority != 0 {
            headers.attr("recurrencepriority", &self.priority.to_string());
        }
        if let Some(misfire) = self.misfire {
            headers.attr("recurrencemisfire", misfire.as_str());
        }
        for (name, value) in [
            ("recurrencestart", &self.start),
            ("recurrenceend", &self.end),
            ("recurrencetimezone", &self.timezone),
            ("recurrencetarget", &self.target),
        ] {
            if let Some(value) = value {
                headers.attr(name, value);
            }
        }
        Ok(headers.list)
    }
}

/// Collects headers, prefixing CloudEvents attribute names.
struct Headers<'a> {
    prefix: &'a str,
    list: Vec<(String, String)>,
}

impl Headers<'_> {
    fn attr(&mut self, name: &str, value: &str) {
        self.list
            .push((format!("{}{name}", self.prefix), value.into()));
    }
}

/// Why a definition could not be encoded.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The recurrenceid is empty or holds `#`.
    InvalidId,
    /// The event lacks its id, source or type.
    IncompleteEvent,
    /// A create or replace without a recurrence.
    MissingRecurrence,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::InvalidId => "recurrenceid is required and must not contain '#'",
            Error::IncompleteEvent => "the event's id, source and type are required",
            Error::MissingRecurrence => "create and replace need a recurrence",
        })
    }
}

impl std::error::Error for Error {}

/// Formats milliseconds as an ISO 8601 duration: `-PT5M`,
/// `PT1H30M`, `P1DT2H`, `PT0.5S`.
pub fn iso_duration(ms: i64) -> String {
    if ms == 0 {
        return "PT0S".into();
    }

    let mut out = String::from(if ms < 0 { "-P" } else { "P" });
    let mut rest = ms.unsigned_abs();

    const DAY: u64 = 86_400_000;
    if rest >= DAY {
        out += &format!("{}D", rest / DAY);
        rest %= DAY;
    }
    if rest == 0 {
        return out;
    }

    out.push('T');
    for (unit, size) in [('H', 3_600_000), ('M', 60_000)] {
        if rest >= size {
            out += &format!("{}{unit}", rest / size);
            rest %= size;
        }
    }
    if rest > 0 {
        let seconds = format!("{}.{:03}", rest / 1000, rest % 1000);
        out += seconds.trim_end_matches('0').trim_end_matches('.');
        out.push('S');
    }
    out
}

/// The CloudEvents attributes of a message's headers, by
/// lowercase name.
pub fn attributes<'a>(
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    prefix: &str,
) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        if name == "content-type" {
            attrs.insert("datacontenttype".into(), value.into());
        } else if let Some(attr) = name.strip_prefix(prefix) {
            attrs.insert(attr.into(), value.into());
        }
    }
    attrs
}

/// Where an occurrence comes from.
#[derive(Debug)]
pub struct Occurrence {
    /// The recurrenceid of its definition.
    pub schedule: String,
    /// Nominal time, RFC 3339, without offset or jitter.
    pub at: String,
    pub revision: u32,
    pub trace: String,
    /// Its event id: the same if it is ever delivered twice.
    pub id: String,
}

/// Reads an occurrence's hydra attributes; `None` for any other
/// event on the subject.
pub fn occurrence(attrs: &HashMap<String, String>) -> Option<Occurrence> {
    let schedule = attrs.get("hydraschedule")?.clone();
    let get = |name: &str| attrs.get(name).cloned().unwrap_or_default();
    Some(Occurrence {
        schedule,
        at: get("hydraoccurrence"),
        revision: get("hydrarevision").parse().unwrap_or(1),
        trace: get("hydratrace"),
        id: get("id"),
    })
}

/// A unique event id and a W3C traceparent, from the clock.
pub fn new_ids(n: u64) -> (String, String) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    let id = format!("evt-{nanos}-{n}");
    let traceparent = format!("00-{nanos:032x}-{n:016x}-01");
    (id, traceparent)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Event {
        Event::new("e1", "/s", "t")
    }

    fn attrs_of(headers: &[(String, String)]) -> HashMap<String, String> {
        attributes(
            headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            NATS_PREFIX,
        )
    }

    #[test]
    fn durations() {
        assert_eq!(iso_duration(-300_000), "-PT5M");
        assert_eq!(iso_duration(5_400_000), "PT1H30M");
        assert_eq!(iso_duration(93_600_000), "P1DT2H");
        assert_eq!(iso_duration(500), "PT0.5S");
        assert_eq!(iso_duration(10_000), "PT10S");
    }

    #[test]
    fn create() {
        let definition = Definition::create("x", Recurrence::Interval(Duration::from_secs(60)))
            .offset_ms(-5_000)
            .misfire(Misfire::CatchUp);
        let attrs = attrs_of(&definition.headers(&event(), NATS_PREFIX).unwrap());

        assert_eq!(attrs["recurrencetype"], "interval");
        assert_eq!(attrs["recurrencerule"], "PT1M");
        assert_eq!(attrs["recurrenceoffset"], "-PT5S");
        assert_eq!(attrs["recurrencemisfire"], "catch_up");
        assert!(!attrs.contains_key("recurrenceaction"));
        assert!(occurrence(&attrs).is_none());
    }

    #[test]
    fn state_change() {
        let pause = Definition::change("x", Action::Pause);
        let attrs = attrs_of(&pause.headers(&event(), NATS_PREFIX).unwrap());

        assert_eq!(attrs["recurrenceaction"], "pause");
        assert!(!attrs.contains_key("recurrencetype"));
    }

    #[test]
    fn invalid() {
        let once = Recurrence::Once("PT1S".into());
        let definition = Definition::create("a#1", once.clone());
        assert_eq!(
            definition.headers(&event(), NATS_PREFIX),
            Err(Error::InvalidId)
        );

        let anonymous = Event::new("", "/s", "t");
        let definition = Definition::create("a", once);
        assert_eq!(
            definition.headers(&anonymous, NATS_PREFIX),
            Err(Error::IncompleteEvent)
        );
    }

    #[test]
    fn subjects() {
        assert_eq!(definition_subject("a.b"), "schedule.a.b");
    }
}
