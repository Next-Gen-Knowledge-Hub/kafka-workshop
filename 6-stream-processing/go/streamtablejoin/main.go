// Command streamtablejoin enriches an order EVENT STREAM with data from a
// customer PROFILE TABLE, materialized locally from its own compacted
// changelog topic — the hand-rolled equivalent of:
//
//	KStream<String, Order> orders = builder.stream("orders");
//	KTable<String, Profile> profiles = builder.table("customer-profiles");
//	orders.join(profiles, (order, profile) -> enrich(order, profile))
//	      .to("enriched-orders");
//
// The table side is consumed continuously and kept as an in-memory map keyed
// by customer ID, exactly mirroring what log compaction already guarantees
// about that topic (only the latest value per key matters). The stream side
// is consumed and, for each order, looked up against that map at the moment
// the order arrives — a stream-table join never waits for a matching window,
// it just asks "what does the table say right now?"
//
// Usage:
//
//	go run . seed-table    # writes customer profiles to customer-profiles
//	go run . seed-stream   # writes order events to orders
//	go run . run           # joins them, writes results to enriched-orders
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker       = "localhost:9092"
	tableTopic   = "customer-profiles"
	streamTopic  = "order-events" // deliberately not "orders" -- that topic belongs to stage 8's capstone pipeline
	outTopic     = "enriched-orders"
	idleShutdown = 5 * time.Second
)

type profile struct {
	CustomerID string `json:"customerId"`
	Name       string `json:"name"`
	Tier       string `json:"tier"`
}

type order struct {
	OrderID    int     `json:"orderId"`
	CustomerID string  `json:"customerId"`
	Amount     float64 `json:"amount"`
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s [seed-table|seed-stream|run]", os.Args[0])
	}
	switch os.Args[1] {
	case "seed-table":
		seedTable()
	case "seed-stream":
		seedStream()
	case "run":
		run()
	default:
		log.Fatalf("unknown command %q, want seed-table|seed-stream|run", os.Args[1])
	}
}

func seedTable() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  tableTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	profiles := []profile{
		{"cust-1", "Alice", "gold"},
		{"cust-2", "Bob", "silver"},
		{"cust-3", "Carol", "gold"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, p := range profiles {
		v, _ := json.Marshal(p)
		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(p.CustomerID), Value: v}); err != nil {
			log.Fatalf("write profile %s: %v", p.CustomerID, err)
		}
	}
	log.Printf("seeded %d customer profiles into %q", len(profiles), tableTopic)
}

func seedStream() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  streamTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	orders := []order{
		{5001, "cust-1", 42.50},
		{5002, "cust-2", 19.99},
		{5003, "cust-3", 128.00},
		{5004, "cust-1", 7.25},
		{5005, "cust-9", 15.00}, // no matching profile — the "table entry missing" case
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, o := range orders {
		v, _ := json.Marshal(o)
		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(o.CustomerID), Value: v}); err != nil {
			log.Fatalf("write order %d: %v", o.OrderID, err)
		}
	}
	log.Printf("seeded %d orders into %q", len(orders), streamTopic)
}

func run() {
	table := struct {
		mu   sync.RWMutex
		data map[string]profile
	}{data: map[string]profile{}}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Table side: materialize customer-profiles into `table`, forever (until stop).
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker}, Topic: tableTopic, GroupID: "streamtablejoin-table",
		})
		defer reader.Close()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			msg, err := reader.FetchMessage(ctx)
			cancel()
			if err != nil {
				select {
				case <-stop:
					return
				default:
					continue // just an idle poll timeout, table topic is long-lived
				}
			}
			var p profile
			if err := json.Unmarshal(msg.Value, &p); err == nil {
				table.mu.Lock()
				table.data[p.CustomerID] = p
				table.mu.Unlock()
				log.Printf("[table] materialized %s -> %+v", p.CustomerID, p)
			}
			reader.CommitMessages(context.Background(), msg)
		}
	}()

	// Give the table a head start so lookups below actually have data to join against —
	// a real Kafka Streams app handles this via the topology's initial state restore.
	time.Sleep(2 * time.Second)

	// Stream side: consume orders, join against the table, publish enriched-orders.
	streamReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: streamTopic, GroupID: "streamtablejoin-stream",
	})
	defer streamReader.Close()

	out := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  outTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond, // publish per-order, not batched — see wordcount's main.go
	}
	defer out.Close()

	joined := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), idleShutdown)
		msg, err := streamReader.FetchMessage(ctx)
		cancel()
		if err != nil {
			log.Println("order stream idle, stopping")
			break
		}

		var o order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			continue
		}

		table.mu.RLock()
		p, found := table.data[o.CustomerID]
		table.mu.RUnlock()

		if !found {
			log.Printf("order %d: no profile for customer %s (yet) — skipping, would retry/DLQ in production", o.OrderID, o.CustomerID)
			streamReader.CommitMessages(context.Background(), msg)
			continue
		}

		enriched := map[string]any{
			"orderId": o.OrderID, "customerId": o.CustomerID, "amount": o.Amount,
			"customerName": p.Name, "customerTier": p.Tier,
		}
		v, _ := json.Marshal(enriched)
		if err := out.WriteMessages(context.Background(), kafka.Message{Key: msg.Key, Value: v}); err != nil {
			log.Printf("publish enriched order: %v", err)
			continue
		}
		joined++
		fmt.Printf("joined: order=%d customer=%s(%s) amount=%.2f\n", o.OrderID, p.Name, p.Tier, o.Amount)
		streamReader.CommitMessages(context.Background(), msg)
	}

	close(stop)
	wg.Wait()
	log.Printf("done: %d orders joined against the customer-profiles table", joined)
}
