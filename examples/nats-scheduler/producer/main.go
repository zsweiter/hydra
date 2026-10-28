// Producer publishes events that carry a recurrence, then walks
// one of them through its lifecycle. It knows nothing about
// hydra but the wire contract (package contract): CloudEvents
// in binary mode, with the recurrence in headers and the body
// left as is. A definition for occurrences on
// notification.reminder is published on
// schedule.notification.reminder, and hydra-scheduler publishes
// each occurrence on notification.reminder.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zsweiter/hydra/contract"
)

// The application's events, and the definitions of the
// occurrences hydra-scheduler adds to them.
const (
	stream    = "NOTIFICATIONS"
	schedules = "SCHEDULES"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	nc, err := nats.Connect(env("NATS_URL", nats.DefaultURL), nats.RetryOnFailedConnect(true))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	// The application owns the streams. The duplicate window
	// of NOTIFICATIONS is what drops an occurrence published
	// twice. SCHEDULES is a work queue: hydra-scheduler is its
	// only consumer, and a definition is removed once stored.
	for _, cfg := range []jetstream.StreamConfig{
		{Name: stream, Subjects: []string{"notification.>"}, Duplicates: 2 * time.Minute},
		{Name: schedules, Subjects: []string{contract.DefinitionSubject("notification.>")}, Retention: jetstream.WorkQueuePolicy, Duplicates: 2 * time.Minute},
	} {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}

	// Definitions published before the scheduler's consumer
	// exists would be skipped (it starts at new messages).
	for _, c := range [][2]string{{schedules, "hydra-scheduler"}, {stream, "app"}} {
		if err := waitForConsumer(ctx, js, c[0], c[1]); err != nil {
			log.Fatal(err)
		}
	}

	p := producer{js: js}

	// Every 10s, six times in total. The body is JSON, but hydra
	// never reads it.
	p.publish(ctx, "notification.adherence", contract.Definition{
		ID: "adherence-p-001", Type: contract.TypeInterval, Rule: "PT10S", Count: 6,
		Misfire: contract.MisfireRunOnce,
	}, "application/json", []byte(`{"patient":"p-001","medication":"metformin 500mg"}`))

	// Every minute, delivered 10s before the minute, in Santiago.
	p.publish(ctx, "notification.reminder", contract.Definition{
		ID: "standup-u-42", Type: contract.TypeCron, Rule: "* * * * *", Offset: -10 * time.Second,
		Timezone: "America/Santiago",
	}, "application/json", []byte(`{"user":"u-42","text":"stand-up in 10 seconds"}`))

	// Once, nominally in 30s, delivered 5s earlier.
	p.publish(ctx, "notification.reminder", contract.Definition{
		ID: "appointment-u-7", Type: contract.TypeOnce, Rule: "PT30S", Offset: -5 * time.Second,
	}, "application/json", []byte(`{"user":"u-7","text":"your appointment is in 5 seconds"}`))

	// Every 20s with a binary body (here a protobuf message):
	// occurrences carry it byte for byte.
	p.publish(ctx, "notification.billing", contract.Definition{
		ID: "sub-9", Type: contract.TypeRRule, Rule: "FREQ=SECONDLY;INTERVAL=20",
	}, "application/x-protobuf", []byte{0x0a, 0x05, 's', 'u', 'b', '-', '9', 0x10, 0xc6, 0x0f})

	// A regular event: the scheduler never sees it, the
	// application consumes it next to the occurrences.
	p.event(ctx, "notification.welcome", []byte(`{"user":"u-42","text":"welcome!"}`))

	// The lifecycle of one schedule. Retrying any of these
	// messages is safe: the same definition changes nothing.
	step := func(wait time.Duration, what string, d contract.Definition, body string) {
		time.Sleep(wait)
		log.Printf("--- %s", what)
		p.publish(ctx, "notification.adherence", d, "application/json", []byte(body))
	}
	adherence := contract.Definition{ID: "adherence-p-001"}

	step(25*time.Second, "pause", with(adherence, contract.ActionPause), "")
	step(20*time.Second, "resume: continues from now, the paused occurrences are skipped", with(adherence, contract.ActionResume), "")
	step(15*time.Second, "replace: every 5s with a new dose (revision 2, revision 1 archived)", contract.Definition{
		Action: contract.ActionReplace, ID: "adherence-p-001", Type: contract.TypeInterval, Rule: "PT5S", Count: 4,
	}, `{"patient":"p-001","medication":"metformin 850mg"}`)
	step(30*time.Second, "cancel the stand-up reminder", contract.Definition{Action: contract.ActionCancel, ID: "standup-u-42"}, "")
	log.Print("done; inspect with: hydra-scheduler schedule notification.adherence adherence-p-001")
}

func with(d contract.Definition, action string) contract.Definition {
	d.Action = action
	return d
}

type producer struct {
	js jetstream.JetStream
	n  int
}

// publish sends a definition of occurrences due on subject. The
// event id must be unique per event; the recurrenceid is what
// identifies the schedule.
func (p *producer) publish(ctx context.Context, subject string, d contract.Definition, contentType string, body []byte) {
	p.n++
	eventID := fmt.Sprintf("%s-%d-%d", d.ID, time.Now().UnixNano(), p.n)
	headers, err := contract.Headers(contract.NATS, contract.Event{
		ID:          eventID,
		Source:      "/examples/producer",
		Type:        "com.acme." + subject,
		ContentType: contentType,
		// A W3C trace makes the change traceable: its trace id
		// is recorded with the schedule's revision.
		TraceParent: fmt.Sprintf("00-%032x-%016x-01", time.Now().UnixNano(), p.n),
	}, d)
	if err != nil {
		log.Fatal(err)
	}

	m := nats.NewMsg(contract.DefinitionSubject(subject))
	m.Data = body
	for k, v := range headers {
		m.Header.Set(k, v)
	}
	// The event id as Nats-Msg-Id: JetStream drops a retried
	// publish of the same event.
	if _, err := p.js.PublishMsg(ctx, m, jetstream.WithMsgID(eventID)); err != nil {
		log.Fatal(err)
	}
	log.Printf("published %s %s %s", subject, headers["ce-recurrenceaction"]+headers["ce-recurrencetype"], d.ID)
}

func (p *producer) event(ctx context.Context, subject string, body []byte) {
	if _, err := p.js.Publish(ctx, subject, body); err != nil {
		log.Fatal(err)
	}
	log.Printf("published %s (regular event)", subject)
}

func waitForConsumer(ctx context.Context, js jetstream.JetStream, stream, name string) error {
	for {
		_, err := js.Consumer(ctx, stream, name)
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			return err
		}
		log.Printf("waiting for consumer %s", name)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
