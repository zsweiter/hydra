// Consumer is a plain JetStream consumer of the NOTIFICATIONS
// stream. It never sees recurrence definitions: those are
// published under schedule.> to a stream of their own, and only
// their occurrences arrive here, next to regular events. The
// hydra headers are read for logging only.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zsweiter/hydra/contract"
)

const stream = "NOTIFICATIONS"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(env("NATS_URL", nats.DefaultURL), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	s, err := waitForStream(ctx, js)
	if err != nil {
		log.Fatal(err)
	}

	cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "app",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "notification.>",
	})
	if err != nil {
		log.Fatal(err)
	}

	cc, err := cons.Consume(func(m jetstream.Msg) {
		defer m.Ack()

		attrs := map[string]string{}
		for k, v := range m.Headers() {
			if name, ok := contract.NATS.Attribute(k); ok && len(v) > 0 {
				attrs[name] = v[0]
			}
		}

		// Handle every message the same way, idempotently by its
		// id: an occurrence keeps it if ever delivered twice.
		// Occurrences also say where they come from.
		if occ, ok, _ := contract.ParseOccurrence(attrs); ok {
			log.Printf("occurrence %-24s schedule=%s at=%s revision=%d trace=%s id=%s %d bytes",
				m.Subject(), occ.Schedule, occ.At.Format(time.RFC3339), occ.Revision, occ.Trace, attrs[contract.AttrID], len(m.Data()))
			return
		}
		log.Printf("event      %-24s %s", m.Subject(), m.Data())
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cc.Stop()

	log.Print("consumer ready")
	<-ctx.Done()
}

func waitForStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	for {
		s, err := js.Stream(ctx, stream)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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
