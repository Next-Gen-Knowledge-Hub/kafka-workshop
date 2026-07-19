// Command transactionalproducer demonstrates the classic exactly-once pattern:
// consume from one topic, transform, produce to another topic, and commit the
// input offsets in the SAME transaction as the output records. Either all of it
// becomes visible (to a read_committed consumer) or none of it does.
//
// Usage:
//
//	go run . seed   # writes sample raw orders to orders-exactly-once-in
//	go run . run    # consumes them transactionally, writes enriched orders to
//	                 # orders-exactly-once-out, one transaction per poll batch
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker    = "localhost:9092"
	inTopic   = "orders-exactly-once-in"
	outTopic  = "orders-exactly-once-out"
	groupID   = "exactly-once-enricher"
	txnPrefix = "enricher-txn-"
)

type rawOrder struct {
	OrderID  int     `json:"orderId"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type enrichedOrder struct {
	OrderID     int     `json:"orderId"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	AmountUSD   float64 `json:"amountUsd"`
	ProcessedAt string  `json:"processedAt"`
}

var fxToUSD = map[string]float64{"USD": 1, "EUR": 1.08, "GBP": 1.27}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s [seed|run]", os.Args[0])
	}

	switch os.Args[1] {
	case "seed":
		seed()
	case "run":
		run()
	default:
		log.Fatalf("unknown command %q, want seed|run", os.Args[1])
	}
}

func seed() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	currencies := []string{"USD", "EUR", "GBP"}
	var records []*kgo.Record
	for i := 0; i < 10; i++ {
		o := rawOrder{OrderID: 1000 + i, Amount: 10 + float64(i)*1.5, Currency: currencies[i%len(currencies)]}
		v, _ := json.Marshal(o)
		records = append(records, &kgo.Record{
			Topic: inTopic,
			Key:   []byte(fmt.Sprintf("%d", o.OrderID)),
			Value: v,
		})
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		log.Fatalf("seed produce: %v", err)
	}
	log.Printf("seeded %d raw orders into %q", len(records), inTopic)
}

func run() {
	session, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics(inTopic),
		kgo.ConsumerGroup(groupID),
		kgo.TransactionalID(txnPrefix+"1"),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
	)
	if err != nil {
		log.Fatalf("new transact session: %v", err)
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log.Println("polling for a batch to process transactionally...")
	fetches := session.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			log.Printf("fetch error: %v", e.Err)
		}
	}

	var records []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	if len(records) == 0 {
		log.Println("nothing to process — run `go run . seed` first")
		return
	}

	if err := session.Begin(); err != nil {
		log.Fatalf("begin transaction: %v", err)
	}
	log.Printf("transaction begun for %d input records", len(records))

	ok := true
	for _, r := range records {
		var in rawOrder
		if err := json.Unmarshal(r.Value, &in); err != nil {
			log.Printf("skipping unparseable record at offset %d: %v", r.Offset, err)
			continue
		}
		rate, known := fxToUSD[in.Currency]
		if !known {
			log.Printf("aborting transaction: unknown currency %q for order %d", in.Currency, in.OrderID)
			ok = false
			break
		}
		out := enrichedOrder{
			OrderID:     in.OrderID,
			Amount:      in.Amount,
			Currency:    in.Currency,
			AmountUSD:   round2(in.Amount * rate),
			ProcessedAt: time.Now().UTC().Format(time.RFC3339),
		}
		v, _ := json.Marshal(out)
		session.Produce(ctx, &kgo.Record{
			Topic: outTopic,
			Key:   r.Key,
			Value: v,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Printf("produce error inside transaction: %v", err)
			}
		})
		log.Printf("enriched order %d: %.2f %s -> $%.2f", in.OrderID, in.Amount, in.Currency, out.AmountUSD)
	}

	end := kgo.TryCommit
	if !ok {
		end = kgo.TryAbort
	}
	committed, err := session.End(ctx, end)
	if err != nil {
		log.Fatalf("end transaction: %v", err)
	}
	if committed {
		log.Println("transaction COMMITTED — enriched orders + input offsets are now atomically visible")
	} else {
		log.Println("transaction ABORTED — no partial output, input offsets unchanged, safe to retry")
	}
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
