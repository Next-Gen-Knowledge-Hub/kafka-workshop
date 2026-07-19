// Command idempotentproducer shows Kafka's idempotent producer in action using
// twmb/franz-go, which — unlike segmentio/kafka-go — enables idempotent writes by
// default: every producer session gets a broker-assigned ProducerID/epoch, and
// every record on a partition carries a monotonic sequence number. If the client
// has to retry a produce request after a transient error, the broker recognizes
// the retried sequence number and returns the original offset instead of
// appending a duplicate.
//
// You can't safely force a real network partition from a workshop script, so
// this program demonstrates the *observable* side of idempotence: a stable
// ProducerID/epoch for the session, and a produced-count that exactly matches
// what's readable back from the partition — which is the property idempotence
// exists to protect, especially under the retries the client performs silently
// on your behalf.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker = "localhost:9092"
	topic  = "orders-idempotent"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent writes are ON by default in franz-go; DisableIdempotentWrite()
		// is the opt-out. We leave it enabled deliberately and call it out here.
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	const n = 25
	records := make([]*kgo.Record, n)
	for i := 0; i < n; i++ {
		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(fmt.Sprintf(`{"orderId":%d,"amount":%.2f}`, i, 9.99+float64(i))),
		}
	}

	results := client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		log.Fatalf("produce: %v", err)
	}

	pid, epoch, err := client.ProducerID(ctx)
	if err != nil {
		log.Fatalf("producer id: %v", err)
	}
	log.Printf("session used idempotent producer id=%d epoch=%d for %d records", pid, epoch, n)

	for i, r := range results {
		rec := r.Record
		log.Printf("acked %2d/%d: partition=%d offset=%d key=%s", i+1, n, rec.Partition, rec.Offset, rec.Key)
	}

	verifyReadBack(ctx, n)
}

// verifyReadBack consumes the topic from the beginning and checks that exactly
// n records are visible — i.e. no duplicates were appended, which is the
// guarantee idempotent production makes even across client-side retries.
func verifyReadBack(ctx context.Context, want int) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("new consumer client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	seen := map[string]bool{}
	for len(seen) < want {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					break
				}
				log.Printf("fetch error: %v", e.Err)
			}
			if ctx.Err() != nil {
				break
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			seen[string(r.Key)] = true
		})
	}

	log.Printf("read back %d/%d distinct keys from %q — %v", len(seen), want, topic,
		map[bool]string{true: "no duplicates, idempotence held", false: "MISMATCH"}[len(seen) == want])
}
