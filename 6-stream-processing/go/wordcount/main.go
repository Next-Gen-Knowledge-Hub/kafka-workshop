// Command wordcount is the "hello world" of stream processing: maintain a
// running count per word, forever, as new lines arrive. It's a hand-rolled
// stand-in for what a Kafka Streams topology would express as
//
//	textLines.flatMapValues(splitToWords)
//	          .groupBy((key, word) -> word)
//	          .count()
//
// The in-memory map IS the KTable: every time a count changes, we republish
// the new (word, count) to word-count-output — that republished stream is the
// table's changelog. A real Kafka Streams app gets a RocksDB-backed,
// changelog-topic-restorable version of this map for free; here it's just a
// map, to keep the concept visible.
//
// Usage:
//
//	go run . seed   # writes a few sentences to word-count-input
//	go run . run    # consumes word-count-input, maintains counts, publishes
//	                 # updates to word-count-output, prints the table on exit
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker    = "localhost:9092"
	inTopic   = "word-count-input"
	outTopic  = "word-count-output"
	idleAfter = 5 * time.Second // stop and print the final table after this much silence
)

var sentences = []string{
	"kafka is a distributed streaming platform",
	"a kafka topic is split into partitions",
	"a partition is an ordered immutable log",
	"kafka streams processes streams of data from kafka",
	"a stream is an unbounded sequence of events",
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
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i, s := range sentences {
		msg := kafka.Message{Key: []byte(fmt.Sprintf("line-%d", i)), Value: []byte(s)}
		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("write line %d: %v", i, err)
		}
	}
	log.Printf("seeded %d lines into %q", len(sentences), inTopic)
}

func run() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    inTopic,
		GroupID:  "wordcount-processor",
		MinBytes: 1,
		MaxBytes: 1 << 20,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  outTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		// Default BatchTimeout is 1s: WriteMessages waits up to that long for a
		// batch to fill before flushing. We publish one small update per word
		// here, so a short timeout keeps each publish snappy instead of stalling
		// the whole read loop behind the batching window.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	counts := map[string]int{}
	log.Println("counting words — Ctrl+C to stop, or wait for the input to go idle")

	for {
		ctx, cancel := context.WithTimeout(sigCtx, idleAfter)
		msg, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			if sigCtx.Err() != nil {
				break // Ctrl+C
			}
			log.Println("input idle, stopping")
			break
		}

		for _, word := range strings.Fields(strings.ToLower(string(msg.Value))) {
			counts[word]++
			update := kafka.Message{
				Key:   []byte(word),
				Value: []byte(fmt.Sprintf("%d", counts[word])),
			}
			if err := writer.WriteMessages(context.Background(), update); err != nil {
				log.Printf("publish update for %q: %v", word, err)
			}
		}

		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}

	printTable(counts)
}

func printTable(counts map[string]int) {
	type kv struct {
		word  string
		count int
	}
	all := make([]kv, 0, len(counts))
	for w, c := range counts {
		all = append(all, kv{w, c})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].count != all[j].count {
			return all[i].count > all[j].count
		}
		return all[i].word < all[j].word
	})

	fmt.Println("\n=== final word-count table ===")
	for _, e := range all {
		fmt.Printf("  %-15s %d\n", e.word, e.count)
	}
}
