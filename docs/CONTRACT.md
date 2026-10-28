# hydra-scheduler wire contract

hydra-scheduler reads recurrence definitions from broker streams and publishes
their occurrences back. Both are **CloudEvents 1.0 in binary content mode**:
every attribute travels in a header and the message body is the event's
`data`, which hydra stores and republishes **byte for byte, without reading
it**. The body can be JSON, Protobuf, Avro or anything else.

The recurrence travels in extension attributes named `recurrence*`. Messages
without them are not definitions: hydra ignores them.

## Subjects

A definition is published on the subject of its occurrences under a prefix,
`schedule.` by default (`HYDRA_SUBJECT_PREFIX`):

| Definition on                                                        | Occurrences on         |
| -------------------------------------------------------------------- | ---------------------- |
| `schedule.notification.billing`                                      | `notification.billing` |
| `schedule.notification.billing` with `recurrencetarget: billing.due` | `billing.due`          |

So consumers of `notification.billing` receive occurrences and the
application's own events, never definitions, and need no filtering. Keep the
definitions in a stream of their own, ideally a work queue: hydra-scheduler is
its only consumer, and a definition is removed once it is stored.

```bash
nats stream add SCHEDULES --subjects 'schedule.>' --retention work --dupe-window 2m --defaults
```

A definition outside the prefix, or a `recurrencetarget` under it (its
occurrences would come back as definitions), is invalid. The schedule is
identified by the subject without the prefix and the `recurrenceid`, so the
same id can be reused on different subjects.

Go producers and consumers can use package
[`contract`](../contract/contract.go), which has no dependencies. The
[clients examples](../examples/clients) show the same in Rust and Node.js.

Producers and consumers can also use an official
[CloudEvents SDK](https://cloudevents.io/) with its NATS binding in binary
mode: [`contract/cetest`](../contract/cetest) checks that definitions encoded
by the Go SDK are scheduled and that occurrences decode with it. hydra itself
does not use the SDK: it reads headers and copies the body, which the SDK
would decode into an event and encode again.

## Headers per broker

| CloudEvents               | NATS JetStream        | Kafka (planned)       |
| ------------------------- | --------------------- | --------------------- |
| Attributes and extensions | header `ce-<name>`    | header `ce_<name>`    |
| `datacontenttype`         | header `content-type` | header `content-type` |
| `data`                    | message body          | record value          |

Header names are compared without case. `datacontenttype` is also accepted as
`ce-datacontenttype`, which is how the Go SDK writes it, and kept as sent.
Kafka producers should key records by `recurrenceid`, so all changes to a
schedule land in one partition, in order; the prefix applies to topics alike.

## Definitions (published by producers)

### CloudEvents attributes

| Attribute                       | Required | Use                                                                                                                                                                                                                                |
| ------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `specversion`                   | yes      | `1.0`                                                                                                                                                                                                                              |
| `id`                            | yes      | Unique per event. Send it as `Nats-Msg-Id` too, so JetStream drops a retried publish                                                                                                                                               |
| `source`                        | yes      | Kept and republished                                                                                                                                                                                                               |
| `type`                          | yes      | Kept and republished                                                                                                                                                                                                               |
| `subject`, `time`, `dataschema` | no       | Kept and republished                                                                                                                                                                                                               |
| `datacontenttype`               | no       | Kept and republished. `application/cloudevents+json` (structured mode) is rejected                                                                                                                                                 |
| `traceparent`, `tracestate`     | no       | [Distributed tracing extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md). The trace id of `traceparent` identifies the change; without it, `id` does. Occurrences inherit both |

Any other header (the application's own) is kept and republished too.

### Recurrence attributes

| Attribute                          | Type      | Required                         | Values                                                                                                                                                                      |
| ---------------------------------- | --------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `recurrenceid`                     | String    | **always**                       | The producer's stable id of the schedule, without `#`. Only the producer knows how to keep it consistent, so hydra never derives it                                         |
| `recurrenceaction`                 | String    | no                               | `create` (default), `replace`, `pause`, `resume`, `cancel`, `delete`                                                                                                        |
| `recurrencetype`                   | String    | create, replace                  | `cron`, `rrule`, `interval`, `once`                                                                                                                                         |
| `recurrencerule`                   | String    | create, replace (but `once` now) | Cron or RRULE expression; for `interval` an ISO 8601 duration (`PT30S`); for `once` an RFC 3339 time or an ISO 8601 delay from when the broker received the event (`PT10M`) |
| `recurrenceoffset`                 | Duration  | no                               | Shift of every occurrence, may be negative: `-PT5M` fires five minutes early                                                                                                |
| `recurrencejitter`                 | Duration  | no                               | Spreads occurrences over `[0, jitter)` with a fixed shift per schedule, to flatten peaks                                                                                    |
| `recurrencestart`, `recurrenceend` | Timestamp | no                               | RFC 3339 bounds of the nominal occurrences. Without a start, `interval` and RRULE are anchored to when the broker received the event                                        |
| `recurrencecount`                  | Integer   | no                               | Maximum occurrences; absent or 0 = unlimited                                                                                                                                |
| `recurrencetimezone`               | String    | no                               | IANA zone (`America/Santiago`); default `UTC`                                                                                                                               |
| `recurrencemisfire`                | String    | no                               | What to do with occurrences missed while no scheduler ran: `skip`, `run_once` (default), `catch_up`                                                                         |
| `recurrencepriority`               | Integer   | no                               | Lower first, when several are due or catching up (default 0)                                                                                                                |
| `recurrencetarget`                 | String    | no                               | Subject to publish occurrences on; default the definition's subject without the prefix. Not under the prefix                                                                |

**Durations** are ISO 8601 with an optional sign: `PT30S`, `PT1H30M`, `P1DT12H`,
`P2W`, `-PT5M`, `PT0.5S`. Years and months are rejected: their length varies.

A `recurrence*` attribute not listed here is an error, so a typo
(`recurrencerul`) is reported instead of silently ignored.

### Actions

| Action    | Effect                                                                                                                      | Repeated                                                                                                                                                                                                        |
| --------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`  | Stores the definition, revision 1                                                                                           | The same definition changes nothing. A different one under the id of a live schedule is rejected (`conflict`): send `replace`. Once the schedule ended, or after `cancel` or `delete`, it starts a new revision |
| `replace` | Archives the current revision as `<subject>\|<recurrenceid>#<n>` (status `archived`) and starts revision n+1, or creates it | The same definition changes nothing                                                                                                                                                                             |
| `pause`   | Stops firing                                                                                                                | No effect                                                                                                                                                                                                       |
| `resume`  | Continues from the next occurrence after now; those due while paused are skipped                                            | No effect                                                                                                                                                                                                       |
| `cancel`  | Stops for good; kept as history                                                                                             | No effect                                                                                                                                                                                                       |
| `delete`  | Removes the schedule, its body and its archived revisions, leaving a tombstone                                              | No effect                                                                                                                                                                                                       |

`pause`, `resume`, `cancel` and `delete` need only `recurrenceaction`,
`recurrenceid` and the required CloudEvents attributes; their body is ignored.

"The same definition" is recognised by a fingerprint of the recurrence
attributes (but the action), `type`, `source`, `datacontenttype`, the target
subject and the body. Event ids, times and traces are left out, so a retry with
a new event id is still a repeat.

### Example (NATS CLI)

```bash
nats pub schedule.notification.billing \
  -H ce-specversion:1.0 -H ce-id:01JD0K7Q2X -H Nats-Msg-Id:01JD0K7Q2X \
  -H ce-source:/billing -H ce-type:com.acme.charge.due \
  -H ce-recurrenceid:sub-9 -H ce-recurrencetype:cron \
  -H 'ce-recurrencerule:0 9 * * *' -H ce-recurrencetimezone:America/Santiago \
  -H ce-recurrenceoffset:-PT5M \
  -H content-type:application/x-protobuf \
  -H ce-traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 \
  < charge.pb

nats pub schedule.notification.billing \
  -H ce-specversion:1.0 -H ce-id:01JD0K9A11 -H ce-source:/billing -H ce-type:com.acme.charge.due \
  -H ce-recurrenceid:sub-9 -H ce-recurrenceaction:pause ''
```

## Occurrences (published by hydra-scheduler)

The body is the definition's, byte for byte. The headers are the definition's,
with every `recurrence*` attribute removed, and:

| Attribute                                                           | Value                                                                                                                                                                          |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`                                                                | Deterministic per occurrence: `<subject>\|<recurrenceid>@<nominal time in Unix nanoseconds>`. Also the `Nats-Msg-Id`, so the stream drops a repeat within its duplicate window |
| `time`                                                              | When it was published                                                                                                                                                          |
| `hydraschedule`                                                     | The `recurrenceid`                                                                                                                                                             |
| `hydraoccurrence`                                                   | Nominal time, RFC 3339, without offset or jitter                                                                                                                               |
| `hydrarevision`                                                     | The revision of the definition that produced it                                                                                                                                |
| `hydratrace`                                                        | The trace id of the change that made that revision                                                                                                                             |
| `traceparent`, `tracestate`                                         | Inherited from the definition, so occurrences join the trace of whoever created the schedule                                                                                   |
| `source`, `type`, `subject`, `datacontenttype`, application headers | As in the definition                                                                                                                                                           |

Consumers need not tell occurrences apart: definitions never reach their
subjects. They should handle occurrences idempotently, keyed by `id`; the
`hydra*` attributes are there to know where an occurrence comes from.

## What hydra-scheduler does with each message

| Message                                                                                                                                                               | Action                                                                   | Log                                    | Metric                                           |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | -------------------------------------- | ------------------------------------------------ |
| No `ce-recurrence*` header (any other event, an occurrence published under the prefix by hand, or a structured-mode event: its attributes are in the body)            | ack, ignore                                                              | `debug`                                | `hydra_messages_ignored_total`                   |
| Outside the prefix, `recurrencetarget` under it, recurrence headers with a structured-mode content type, missing required attribute, unknown `recurrence*`, bad value | term (not redelivered)                                                   | `warn` with the reason                 | `hydra_definitions_total{result="invalid"}`      |
| `create` for a live schedule with another definition                                                                                                                  | term                                                                     | `warn`                                 | `hydra_definitions_total{result="conflict"}`     |
| `pause`/`resume` for an unknown id                                                                                                                                    | redelivered for 30 s (its create may be processing elsewhere), then term | `debug` while redelivered, then `warn` | `hydra_definitions_total{result="unknown"}`      |
| Older than the stored change (when the stream stored it)                                                                                                              | ack, ignore                                                              | `debug`                                | `hydra_definitions_total{result="stale"}`        |
| Valid                                                                                                                                                                 | stored, then ack                                                         | `debug`                                | `hydra_definitions_total` by result              |
| The store fails                                                                                                                                                       | redelivered                                                              | `error`, once per batch                | `hydra_store_errors_total{component="consumer"}` |

Definitions are stored in batches: a message is acknowledged only after the
batch holding it is durable in the store, so an acknowledged definition is
never lost. An occurrence is claimed (its schedule advanced) only after the
broker acknowledged its publish; until then it is published again with
exponential backoff, so an occurrence is never lost either.

## Guarantees

- **No loss.** Definitions are acknowledged after the store commits;
  occurrences are claimed after the broker acknowledges them.
- **No visible duplicates** within the stream's duplicate window: the
  occurrence id is deterministic, and JetStream drops a repeat. A repeat is
  published when an occurrence was published but its claim failed, for example
  while the store is unreachable; widen the stream's `duplicate_window` if that
  can last longer than two minutes.
- **Order per schedule.** Changes carry as a version when the stream stored
  them; one older than the stored change is ignored, so instances processing
  messages out of order converge on the newest. Unlike the stream sequence,
  that time keeps growing when the stream is recreated or definitions move to
  another stream. It comes from the NATS server's clock: two changes of one
  schedule stored within a clock step back (or, in a cluster, across a
  leader change between servers whose clocks differ) could be applied out
  of order.
- **Consumers recover.** A consumer deleted, or whose stream was recreated,
  is created again within `CheckInterval` (10 s) or as soon as heartbeats
  stop: from the start of a work queue or a new stream, otherwise from when it
  was last seen (a redelivered change is recognised and ignored). Meanwhile
  `/readyz` fails and `hydra_streams_down` counts it.
