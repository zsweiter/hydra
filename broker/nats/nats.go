// Package natsbroker connects a hydra bridge to NATS
// JetStream: it consumes recurrence definitions from one or
// more streams and publishes occurrences back.
package natsbroker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zsweiter/hydra/bridge"
	"github.com/zsweiter/hydra/pkg/logger"
)

// Handler processes inbound messages: a *bridge.Bridge.
type Handler interface {
	Handle(ctx context.Context, msgs []bridge.Message) []bridge.Result
	Key(msg bridge.Message) string
}

// Config selects what the scheduler consumes.
type Config struct {
	// Streams are the JetStream streams to consume. At least
	// one is required.
	Streams []string

	// Subjects filters the streams: each stream is consumed
	// through the filters that overlap its subjects. Empty
	// means every subject.
	Subjects []string

	// Consumer is the durable consumer name, the same on every
	// stream. Default "hydra-scheduler".
	Consumer string

	// DeliverAll makes a newly created consumer start from the
	// beginning of its stream instead of new messages only.
	// It has no effect once the consumer exists.
	DeliverAll bool

	// AckWait bounds how long a definition may take to be
	// stored before it is redelivered. Default 30s.
	AckWait time.Duration

	// RetryDelay is how long a definition that failed to be
	// stored waits before redelivery. Default 5s.
	RetryDelay time.Duration

	// Workers is how many batches are processed at once, at
	// most, and Batch how many messages a batch holds at most.
	// Messages of one schedule always go to the same worker, so
	// its changes apply in order. A worker takes whatever is
	// waiting: one message when idle, a full batch under load.
	// Workers start with the first message for them and stop
	// after IdleTimeout without any. Defaults 16, 256 and 30s.
	Workers     int
	Batch       int
	IdleTimeout time.Duration

	// CheckInterval is how often each consumer is checked to
	// still exist; one deleted (with its stream, or by hand) is
	// created again. Default 10s.
	CheckInterval time.Duration

	// Health, if set, reports the streams not being consumed
	// and how often consumers were recreated.
	Health *Health

	Logger logger.Logger
}

// Health reports whether Consume is consuming every stream.
// Its zero value is ready to use.
type Health struct {
	mu       sync.Mutex
	down     map[string]bool
	restarts atomic.Int64
	workers  atomic.Int64
}

func (h *Health) set(stream string, consuming bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.down == nil {
		h.down = map[string]bool{}
	}
	if consuming {
		delete(h.down, stream)
	} else {
		h.down[stream] = true
	}
}

// Down returns the streams not being consumed: not created
// yet, or whose consumer is being recreated.
func (h *Health) Down() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var down []string
	for name := range h.down {
		down = append(down, name)
	}
	slices.Sort(down)
	return down
}

// Restarts returns how many times a lost consumer was created
// again.
func (h *Health) Restarts() int64 {
	return h.restarts.Load()
}

// Workers returns how many workers are running now.
func (h *Health) Workers() int64 {
	return h.workers.Load()
}

func (h *Health) addWorker(n int64) {
	if h != nil {
		h.workers.Add(n)
	}
}

func (c *Config) defaults() {
	if c.Consumer == "" {
		c.Consumer = "hydra-scheduler"
	}
	if c.AckWait <= 0 {
		c.AckWait = 30 * time.Second
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 5 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 16
	}
	if c.Batch <= 0 {
		c.Batch = 256
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.CheckInterval <= 0 {
		c.CheckInterval = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = logger.DefaultLogger
	}
}

// Publisher publishes occurrences to JetStream, using the
// occurrence id as Nats-Msg-Id so the stream drops a repeat
// within its deduplication window.
type Publisher struct {
	js jetstream.JetStream

	// Timeout bounds how long PublishBatch waits for the
	// acknowledgements; unacknowledged messages are reported as
	// failed and delivered again later. Default 10s.
	Timeout time.Duration
}

func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js, Timeout: 10 * time.Second}
}

var _ bridge.BatchPublisher = (*Publisher)(nil)

func (p *Publisher) Publish(ctx context.Context, msg bridge.Message) error {
	_, err := p.js.PublishMsg(ctx, natsMsg(msg), jetstream.WithMsgID(msg.ID))
	return err
}

// ackSlices reuses the futures slice of PublishBatch.
var ackSlices = sync.Pool{New: func() any { return new([]jetstream.PubAckFuture) }}

// PublishBatch publishes msgs asynchronously, so the batch
// costs about one round trip instead of one per message, and
// waits for every acknowledgement (or ctx).
func (p *Publisher) PublishBatch(ctx context.Context, msgs []bridge.Message) []error {
	if p.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.Timeout)
		defer cancel()
	}

	var errs []error
	fail := func(i int, err error) {
		if errs == nil {
			errs = make([]error, len(msgs))
		}
		errs[i] = err
	}

	ackp := ackSlices.Get().(*[]jetstream.PubAckFuture)
	acks := slices.Grow((*ackp)[:0], len(msgs))[:len(msgs)]
	defer func() {
		clear(acks)
		*ackp = acks[:0]
		ackSlices.Put(ackp)
	}()
	for i, msg := range msgs {
		f, err := p.js.PublishMsgAsync(natsMsg(msg), jetstream.WithMsgID(msg.ID))
		if err != nil {
			fail(i, err)
			continue
		}
		acks[i] = f
	}

	for i, f := range acks {
		if f == nil {
			continue
		}
		select {
		case <-f.Ok():
		case err := <-f.Err():
			fail(i, err)
		case <-ctx.Done():
			fail(i, ctx.Err())
		}
	}
	return errs
}

// natsMsg wraps msg without copying: the header map becomes the
// message's, as the bridge builds a new one per message.
func natsMsg(msg bridge.Message) *nats.Msg {
	return &nats.Msg{Subject: msg.Subject, Data: msg.Data, Header: nats.Header(msg.Header)}
}

// Consume feeds the messages of the configured streams to h
// until ctx is cancelled. It waits for the streams to exist,
// since stream ownership belongs to the application.
//
// Messages are acknowledged once h has processed them (a
// definition is then durable), terminated when h discards them,
// and redelivered after RetryDelay when h asks to retry.
func Consume(ctx context.Context, js jetstream.JetStream, cfg Config, h Handler) error {
	cfg.defaults()
	if len(cfg.Streams) == 0 {
		return errors.New("natsbroker: no stream to consume")
	}

	// One slot per worker, chosen by schedule key, so the
	// changes of a schedule go through one queue, in order. A
	// slot's goroutine starts with its first message and ends
	// after IdleTimeout without any; at most one runs per slot.
	var workers sync.WaitGroup
	slots := make([]*slot, cfg.Workers)
	for i := range slots {
		slots[i] = &slot{queue: make(chan jetstream.Msg, cfg.Batch)}
	}
	defer func() {
		for _, s := range slots {
			close(s.queue)
		}
		workers.Wait()
	}()

	dispatch := func(m jetstream.Msg) {
		key := h.Key(bridge.Message{Subject: m.Subject(), Header: bridge.Header(m.Headers())})
		s := slots[fnv32a(key)%uint32(len(slots))]
		s.queue <- m
		s.start(func() {
			workers.Add(1)
			cfg.Health.addWorker(1)
			go func() {
				defer workers.Done()
				defer cfg.Health.addWorker(-1)
				s.work(ctx, cfg, h)
			}()
		})
	}

	for _, name := range cfg.Streams {
		cfg.Health.set(name, false)
	}

	// Every stream is started before consuming any, so a
	// misconfiguration fails at once; later losses are
	// recovered by supervise.
	var streams sync.WaitGroup
	defer streams.Wait()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, name := range cfg.Streams {
		c, err := startStream(streamCtx, js, name, cfg, dispatch, nil)
		if err != nil {
			return err
		}
		streams.Add(1)
		go func() {
			defer streams.Done()
			supervise(streamCtx, c)
		}()
	}

	<-ctx.Done()
	return ctx.Err()
}

// consumption is one stream being consumed.
type consumption struct {
	js       jetstream.JetStream
	name     string
	cfg      Config
	dispatch func(jetstream.Msg)
	log      logger.Logger

	cc      jetstream.ConsumeContext
	created time.Time // of the stream, to tell a recreated one
	seen    time.Time // last time the consumer was known to exist
	check   chan struct{}
}

// startStream waits for the stream, creates or updates the
// consumer and starts consuming. A work-queue stream (the
// recommended retention for definitions) is always consumed
// from its start: it holds only what was not acknowledged.
// Otherwise prev is the consumption that was lost, if any: a
// recreated stream is consumed from its start, since all of it
// is new; a consumer lost from the same stream resumes from
// when it was last known to exist, and what it redelivers is
// ignored as already applied.
func startStream(ctx context.Context, js jetstream.JetStream, name string, cfg Config, dispatch func(jetstream.Msg), prev *consumption) (*consumption, error) {
	log := cfg.Logger.With(logger.Fields{"stream": name, "consumer": cfg.Consumer})

	stream, err := waitForStream(ctx, js, name, log)
	if err != nil {
		return nil, err
	}
	created := stream.CachedInfo().Created

	filters, err := filtersFor(stream.CachedInfo().Config.Subjects, cfg.Subjects)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", name, err)
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:        cfg.Consumer,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        cfg.AckWait,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		FilterSubjects: filters,
		// Enough in flight for every worker to fill a batch.
		MaxAckPending: max(cfg.Workers*cfg.Batch*2, 1000),
	}
	switch {
	case stream.CachedInfo().Config.Retention == jetstream.WorkQueuePolicy:
		// JetStream requires it, and it is what a work queue
		// means: what is left is what was not acknowledged.
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case prev != nil && !created.Equal(prev.created):
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case prev != nil:
		from := prev.seen.Add(-time.Second)
		consumerCfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		consumerCfg.OptStartTime = &from
	case cfg.DeliverAll:
		consumerCfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	cons, err := stream.Consumer(ctx, cfg.Consumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		cons, err = stream.CreateConsumer(ctx, consumerCfg)
	} else if err == nil {
		// The deliver policy of an existing consumer cannot
		// change; keep it and update the rest.
		info := cons.CachedInfo().Config
		consumerCfg.DeliverPolicy, consumerCfg.OptStartTime = info.DeliverPolicy, info.OptStartTime
		cons, err = stream.UpdateConsumer(ctx, consumerCfg)
	}
	if err != nil {
		return nil, fmt.Errorf("stream %s, consumer %s: %w", name, cfg.Consumer, err)
	}

	c := &consumption{
		js: js, name: name, cfg: cfg, dispatch: dispatch, log: log,
		created: created, seen: time.Now(), check: make(chan struct{}, 1),
	}
	c.cc, err = cons.Consume(dispatch, jetstream.ConsumeErrHandler(c.onError))
	if err != nil {
		return nil, fmt.Errorf("stream %s: consume: %w", name, err)
	}
	cfg.Health.set(name, true)
	log.Info("consuming", logger.Fields{"subjects": filters, "deliver": consumerCfg.DeliverPolicy.String()})
	return c, nil
}

// onError asks for a check when the consumer may be gone: the
// server said it was deleted, or heartbeats stopped (a server
// that lost its state never says so).
func (c *consumption) onError(_ jetstream.ConsumeContext, err error) {
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrNoHeartbeat) {
		select {
		case c.check <- struct{}{}:
		default:
		}
	}
}

// supervise checks that the consumer of c still exists and
// creates it again when it does not, until ctx ends.
func supervise(ctx context.Context, c *consumption) {
	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.cc.Drain()
			<-c.cc.Closed()
			return
		case <-ticker.C:
		case <-c.check:
		case <-c.cc.Closed():
		}

		err := c.exists(ctx)
		if err == nil || ctx.Err() != nil {
			continue
		}
		c.cc.Stop()
		c.cfg.Health.set(c.name, false)
		c.log.Warn("consumer lost, creating it again", logger.Fields{"error": err.Error()})

		for delay := 500 * time.Millisecond; ; delay = min(delay*2, 10*time.Second) {
			next, err := startStream(ctx, c.js, c.name, c.cfg, c.dispatch, c)
			if err == nil {
				c.cfg.Health.restarts.Add(1)
				c = next
				break
			}
			if ctx.Err() != nil {
				return
			}
			c.log.Error(err, logger.Fields{"retry_in": delay.String()})
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

// exists returns an error when the consumer or its stream are
// gone, and nil when it exists or that cannot be told now (a
// disconnection, which the client recovers from by itself).
func (c *consumption) exists(ctx context.Context) error {
	select {
	case <-c.cc.Closed():
		return errors.New("consume stopped")
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := c.js.Stream(ctx, c.name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	if err != nil {
		return nil
	}
	if !stream.CachedInfo().Created.Equal(c.created) {
		return errors.New("stream recreated")
	}
	if _, err := stream.Consumer(ctx, c.cfg.Consumer); errors.Is(err, jetstream.ErrConsumerNotFound) {
		return err
	} else if err != nil {
		return nil
	}
	c.seen = time.Now()
	return nil
}

// slot is the queue of one worker.
type slot struct {
	queue   chan jetstream.Msg
	running atomic.Bool
}

// start runs spawn unless the slot's worker is running.
func (s *slot) start(spawn func()) {
	if s.running.CompareAndSwap(false, true) {
		spawn()
	}
}

// fnv32a hashes key (FNV-1a) without allocating.
func fnv32a(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// work processes the messages of the slot in batches: it waits
// for one, takes whatever else is waiting up to Batch, hands
// them to h together and acknowledges each by its result. It
// returns when the queue is closed, or after IdleTimeout
// without messages.
func (s *slot) work(ctx context.Context, cfg Config, h Handler) {
	batch := make([]jetstream.Msg, 0, cfg.Batch)
	valid := make([]jetstream.Msg, 0, cfg.Batch)
	msgs := make([]bridge.Message, 0, cfg.Batch)
	idle := time.NewTimer(cfg.IdleTimeout)
	defer idle.Stop()

	for {
		var m jetstream.Msg
		select {
		case next, ok := <-s.queue:
			if !ok {
				return
			}
			m = next
		case <-idle.C:
			// A message dispatched after the check below finds
			// the slot stopped and starts a worker; one
			// dispatched before it is taken here.
			s.running.Store(false)
			if len(s.queue) == 0 || !s.running.CompareAndSwap(false, true) {
				return
			}
			idle.Reset(cfg.IdleTimeout)
			continue
		}

		batch = append(batch[:0], m)
	drain:
		for len(batch) < cfg.Batch {
			select {
			case m, ok := <-s.queue:
				if !ok {
					break drain
				}
				batch = append(batch, m)
			default:
				break drain
			}
		}

		msgs, valid = msgs[:0], valid[:0]
		for _, m := range batch {
			msg, err := toMessage(m)
			if err != nil {
				cfg.Logger.Error(fmt.Errorf("read message metadata: %w", err))
				m.NakWithDelay(cfg.RetryDelay)
				continue
			}
			msgs = append(msgs, msg)
			valid = append(valid, m)
		}

		// The handler logs why it discards or retries; this only
		// records how many are redelivered.
		retried := 0
		for i, r := range h.Handle(ctx, msgs) {
			m := valid[i]
			switch r.Outcome {
			case bridge.Ack:
				m.Ack()
			case bridge.Discard:
				m.Term()
			default:
				retried++
				m.NakWithDelay(cfg.RetryDelay)
			}
		}
		if retried > 0 {
			cfg.Logger.Debug("redelivering messages", logger.Fields{"messages": retried, "retry_in": cfg.RetryDelay.String()})
		}

		// Drop the references before waiting, so idle slots hold
		// no messages for the GC.
		clear(batch)
		clear(valid)
		clear(msgs)
		idle.Reset(cfg.IdleTimeout)
	}
}

func waitForStream(ctx context.Context, js jetstream.JetStream, name string, log logger.Logger) (jetstream.Stream, error) {
	delay := 500 * time.Millisecond
	for {
		stream, err := js.Stream(ctx, name)
		if err == nil {
			return stream, nil
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("stream %s: %w", name, err)
		}

		log.Info("waiting for stream", logger.Fields{"retry_in": delay.String()})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 10*time.Second)
	}
}

// filtersFor returns the filters that apply to a stream with the
// given subjects: those overlapping one of them. No filters
// means the whole stream; filters that match nothing in it are
// an error, as the stream would then be consumed for nothing.
func filtersFor(streamSubjects, filters []string) ([]string, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	var out []string
	for _, f := range filters {
		for _, s := range streamSubjects {
			if subjectsCollide(f, s) {
				out = append(out, f)
				break
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("none of the subjects %v is in the stream (%v)", filters, streamSubjects)
	}
	return out, nil
}

// subjectsCollide reports whether two NATS subjects, with
// wildcards, can match a common subject: "*" matches one token,
// ">" one or more at the end.
func subjectsCollide(a, b string) bool {
	at, bt := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; ; i++ {
		switch {
		case i == len(at) && i == len(bt):
			return true
		case i == len(at) || i == len(bt):
			return false
		case at[i] == ">" || bt[i] == ">":
			return true
		case at[i] == "*" || bt[i] == "*" || at[i] == bt[i]:
			continue
		default:
			return false
		}
	}
}

// toMessage converts a JetStream message. Nats-* headers are
// transport controls (dedup id, expectations) and are not
// carried over to occurrences.
func toMessage(m jetstream.Msg) (bridge.Message, error) {
	meta, err := m.Metadata()
	if err != nil {
		return bridge.Message{}, err
	}

	header := bridge.Header{}
	for k, v := range m.Headers() {
		if strings.HasPrefix(k, "Nats-") {
			continue
		}
		header[k] = append([]string(nil), v...)
	}

	return bridge.Message{
		Subject: m.Subject(),
		Header:  header,
		Data:    m.Data(),
		ID:      fmt.Sprintf("%s:%d", meta.Stream, meta.Sequence.Stream),
		Time:    meta.Timestamp,
		// When the stream stored it orders the changes of a
		// schedule even when instances process them out of
		// order. Unlike the sequence, it keeps growing when the
		// stream is recreated or definitions move to another
		// stream; it relies on the server's clock not stepping
		// back between two changes of one schedule.
		Version: meta.Timestamp.UnixNano(),
	}, nil
}
