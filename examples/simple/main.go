package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	hydra "github.com/zsweiter/hydra"
)

func main() {
	q := hydra.New(hydra.Config{
		MinWorkers: 2,
		MaxWorkers: 4,
		BatchSize:  10,
		LeaseTTL:   time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var processed atomic.Int64

	q.Handle("emails", func(ctx context.Context, job *hydra.Job) error {
		n := processed.Add(1)
		fmt.Printf("[%d] Processing email: %s\n", n, job.Name)
		return nil
	})

	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer q.Stop(ctx)

	for i := 1; i <= 5; i++ {
		job := hydra.NewJob("", fmt.Sprintf("email-%d", i), []byte("hello"), nil)
		if err := q.Submit(ctx, "emails", job); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Submitted: email-%d\n", i)
	}

	time.Sleep(2 * time.Second)
	fmt.Printf("\nProcessed %d jobs\n", processed.Load())
}
