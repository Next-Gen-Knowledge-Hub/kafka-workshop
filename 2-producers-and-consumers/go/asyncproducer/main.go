// Command asyncproducer fires off a burst of messages without waiting for each one
// to be acknowledged individually. Completions are reported later, in batches, via
// the Writer's Completion callback — the shape you want for high-throughput pipelines.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "page-views"
)

func main() {
	var acked, failed int64
	var wg sync.WaitGroup

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async:                  true, // WriteMessages returns immediately; delivery is reported below.
		Completion: func(messages []kafka.Message, err error) {
			defer wg.Add(-len(messages))
			if err != nil {
				atomic.AddInt64(&failed, int64(len(messages)))
				log.Printf("batch of %d failed: %v", len(messages), err)
				return
			}
			atomic.AddInt64(&acked, int64(len(messages)))
			for _, m := range messages {
				log.Printf("acked async: partition=%d offset=%d key=%s", m.Partition, m.Offset, m.Key)
			}
		},
	}
	defer writer.Close() // Close() also waits for in-flight async writes to complete.

	start := time.Now()
	const total = 100
	for i := 0; i < total; i++ {
		wg.Add(1)
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("session-%d", i%10)),
			Value: []byte(fmt.Sprintf(`{"event":"view","seq":%d}`, i)),
		}
		// WriteMessages does not block on the network round trip when Async is true;
		// it only blocks briefly to enqueue the message into the writer's batch buffer.
		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Printf("enqueue %d failed: %v", i, err)
			wg.Done()
		}
	}

	log.Printf("enqueued %d messages in %s, waiting for broker acks...", total, time.Since(start))
	wg.Wait()
	log.Printf("done in %s: acked=%d failed=%d", time.Since(start), atomic.LoadInt64(&acked), atomic.LoadInt64(&failed))
}
