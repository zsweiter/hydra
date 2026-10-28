# Clients of hydra-scheduler

Producers and consumers only talk to the broker. To schedule something, a
producer publishes a CloudEvent in binary mode whose headers carry a
recurrence (see [docs/CONTRACT.md](../../docs/CONTRACT.md)) on the subject of
its occurrences under the `schedule.` prefix. Consumers receive the
occurrences on the subject without it, with the same body, next to any other
event: they never see definitions, so they need no filtering.

Each client has a small module implementing the contract with no extra
dependencies, which you can copy into your project:

| Language | Contract module | Producer | Consumer | Broker client |
|---|---|---|---|---|
| Go | [`github.com/zsweiter/hydra/contract`](../../contract) | [producer](../nats-scheduler/producer/main.go) | [consumer](../nats-scheduler/consumer/main.go) | `nats.go` |
| Rust | [`src/contract.rs`](rust/src/contract.rs) | [producer](rust/src/bin/producer.rs) | [consumer](rust/src/bin/consumer.rs) | `async-nats` |
| Node.js | [`hydra.mjs`](node/hydra.mjs) | [producer](node/producer.mjs) | [consumer](node/consumer.mjs) | `@nats-io/jetstream` |

Every producer creates two streams, `NOTIFICATIONS` (`notification.>`, the
events and occurrences) and `SCHEDULES` (`schedule.notification.>`, the
definitions, a work queue), and runs the same scenario:

1. `adherence-p-001`: every 10 s, six times.
2. `standup-u-42`: every minute, 10 s early, in `America/Santiago`.
3. `appointment-u-7`: once, in 30 s.
4. Then `adherence-p-001` is paused, resumed (the occurrences due while paused
   are skipped) and replaced (revision 2, every 5 s, four times; revision 1 is
   archived), and `standup-u-42` is cancelled.

The Go producer also schedules `sub-9` with a Protobuf body, to show the body
is carried byte for byte, and publishes a regular event on `NOTIFICATIONS`,
which consumers receive next to the occurrences.

## Running

Start NATS and hydra-scheduler, then a consumer and a producer of any
language (they interoperate: the contract is the same).

```bash
docker run -d --name nats -p 4222:4222 nats:2.15.1-alpine -js
make scheduler-sqlite
HYDRA_STREAMS=SCHEDULES HYDRA_STORE=sqlite:hydra.db ./bin/hydra-scheduler-sqlite &
```

hydra-scheduler waits for `SCHEDULES` to exist; the producers create it. As a
work queue, it is consumed from its start, so nothing published before the
scheduler's consumer exists is missed.

```bash
# Go (or the whole demo: docker compose -f examples/nats-scheduler/docker-compose.yml up --build)
go run ./examples/nats-scheduler/consumer &
go run ./examples/nats-scheduler/producer

# Rust
cd examples/clients/rust
cargo run --bin consumer &
cargo run --bin producer

# Node.js 20+
cd examples/clients/node
npm install
node consumer.mjs &
node producer.mjs
```

Then inspect a schedule and its revisions:

```bash
HYDRA_STORE=sqlite:hydra.db ./bin/hydra-scheduler-sqlite schedule notification.adherence adherence-p-001
```

## Writing your own

- Publish a definition on `schedule.<subject>` (`definitionSubject`,
  `definition_subject`, `contract.DefinitionSubject`); its occurrences go to
  `<subject>`, or to `recurrencetarget`.
- `recurrenceid` is yours: derive it from your domain (`invoice-1234`,
  `user-42-standup`) so a retry carries the same id. Sending the same
  definition again changes nothing; sending a different one under a live id is
  rejected unless `recurrenceaction` is `replace`.
- Use a new CloudEvents `id` per event and send it as `Nats-Msg-Id`, so
  JetStream drops a publish you retried.
- Send a W3C `traceparent` (header `ce-traceparent`): its trace id is recorded
  with each revision and state change, and occurrences inherit it.
- Durations are ISO 8601: `PT30S`, `-PT5M`, `P1D`.
- Consumers: handle occurrences idempotently, keyed by their `ce-id`, which is
  the same if an occurrence is ever delivered twice.
