// Command windowedaggregation buckets stock ticks into 5-second EVENT-TIME
// tumbling windows per symbol and computes min/max/avg price per window — the
// hand-rolled equivalent of:
//
//	ticks.groupByKey()
//	     .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
//	     .aggregate(...)
//
// Deliberately using the timestamp embedded in each tick's JSON payload
// (event time) rather than wall-clock time or the Kafka record's own
// broker-assigned timestamp (processing time) — see CONCEPTS.md's section on
// time. Since the seeded data is a finite batch rather than a live feed, this
// reads everything available, then closes every window and prints/publishes
// results — the same outcome a real windowed aggregation reaches once its
// watermark passes each window's end.
//
// Usage:
//
//	go run . seed   # writes synthetic ticks spanning several windows, some out of order
//	go run . run    # aggregates them per symbol per window
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker       = "localhost:9092"
	inTopic      = "stock-ticks"
	outTopic     = "stock-window-output"
	windowSize   = 5 * time.Second
	idleShutdown = 5 * time.Second
)

type tick struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	EventTime int64   `json:"eventTimeMs"` // epoch millis — the event's own clock, not Kafka's
}

type windowKey struct {
	Symbol string
	Start  int64 // window start, epoch millis
}

type windowAgg struct {
	Count int
	Sum   float64
	Min   float64
	Max   float64
}

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
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  inTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	base := time.Now().Add(-30 * time.Second).UnixMilli() // pretend these happened over the last 30s
	ticks := []tick{
		{"ACME", 100.0, base + 0},
		{"ACME", 101.5, base + 1200},
		{"ACME", 99.8, base + 3900},
		{"ACME", 102.0, base + 900}, // arrives "late" relative to the one above, still same window
		{"ACME", 103.2, base + 6100},
		{"ACME", 104.0, base + 8700},
		{"WIDGET", 50.0, base + 500},
		{"WIDGET", 49.5, base + 4400},
		{"WIDGET", 51.0, base + 5200},
		{"WIDGET", 52.5, base + 12100},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, t := range ticks {
		v, _ := json.Marshal(t)
		msg := kafka.Message{Key: []byte(t.Symbol), Value: v}
		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("write tick %d: %v", i, err)
		}
	}
	log.Printf("seeded %d ticks into %q", len(ticks), inTopic)
}

func run() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    inTopic,
		GroupID:  "windowed-aggregation",
		MinBytes: 1,
		MaxBytes: 1 << 20,
	})
	defer reader.Close()

	windows := map[windowKey]*windowAgg{}
	log.Println("aggregating ticks into 5s tumbling windows...")

	for {
		ctx, cancel := context.WithTimeout(context.Background(), idleShutdown)
		msg, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			log.Println("input idle, closing all windows")
			break
		}

		var t tick
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("skipping unparseable tick: %v", err)
			continue
		}

		start := windowStart(t.EventTime)
		key := windowKey{Symbol: t.Symbol, Start: start}
		agg, ok := windows[key]
		if !ok {
			agg = &windowAgg{Min: math.Inf(1), Max: math.Inf(-1)}
			windows[key] = agg
		}
		agg.Count++
		agg.Sum += t.Price
		agg.Min = math.Min(agg.Min, t.Price)
		agg.Max = math.Max(agg.Max, t.Price)

		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}

	publishAndPrint(windows)
}

func windowStart(eventTimeMs int64) int64 {
	size := windowSize.Milliseconds()
	return (eventTimeMs / size) * size
}

func publishAndPrint(windows map[windowKey]*windowAgg) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  outTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	keys := make([]windowKey, 0, len(windows))
	for k := range windows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Symbol != keys[j].Symbol {
			return keys[i].Symbol < keys[j].Symbol
		}
		return keys[i].Start < keys[j].Start
	})

	fmt.Println("\n=== closed windows (symbol, [start,end), count, min, max, avg) ===")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, k := range keys {
		agg := windows[k]
		avg := agg.Sum / float64(agg.Count)
		start := time.UnixMilli(k.Start).UTC().Format("15:04:05")
		end := time.UnixMilli(k.Start + windowSize.Milliseconds()).UTC().Format("15:04:05")
		fmt.Printf("  %-8s [%s,%s) count=%d min=%.2f max=%.2f avg=%.2f\n",
			k.Symbol, start, end, agg.Count, agg.Min, agg.Max, avg)

		out := map[string]any{
			"symbol": k.Symbol, "windowStart": k.Start, "count": agg.Count,
			"min": agg.Min, "max": agg.Max, "avg": avg,
		}
		v, _ := json.Marshal(out)
		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(k.Symbol), Value: v}); err != nil {
			log.Printf("publish window result: %v", err)
		}
	}
}
