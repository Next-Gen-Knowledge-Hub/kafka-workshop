// Command keyedpartitioning shows two things:
//  1. Messages sharing a key always land on the same partition (kafka.Hash, the
//     default keyed balancer).
//  2. How to write a custom Balancer — here, one that sends a VIP customer's
//     traffic to a dedicated partition and hashes everyone else across the rest.
package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "customer-events"
)

// vipBalancer routes a fixed set of VIP customer IDs to partition 0, and spreads
// everyone else across the remaining partitions by hashing the key. It implements
// kafka.Balancer.
type vipBalancer struct {
	vips map[string]bool
}

func (b vipBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if b.vips[string(msg.Key)] {
		return 0 // dedicated "VIP lane" partition
	}
	h := fnv.New32a()
	h.Write(msg.Key)
	rest := partitions[1:] // everyone else shares the non-VIP partitions
	if len(rest) == 0 {
		return partitions[0]
	}
	return rest[int(h.Sum32())%len(rest)]
}

func main() {
	demoDefaultHashing()
	demoCustomBalancer()
}

// demoDefaultHashing proves that the same key always maps to the same partition.
func demoDefaultHashing() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // key-based partitioner, same key -> same partition
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	users := []string{"alice", "bob", "alice", "carol", "bob", "alice"}
	for i, user := range users {
		msg := kafka.Message{
			Key:   []byte(user),
			Value: []byte(fmt.Sprintf(`{"seq":%d,"action":"click"}`, i)),
		}
		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Fatalf("write: %v", err)
		}
	}
	log.Println("sent keyed messages — check Kafka UI: every 'alice' message is on the same partition")
}

// demoCustomBalancer sends a mix of VIP and regular customer events using our
// custom Balancer implementation.
func demoCustomBalancer() {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: topic,
		Balancer: vipBalancer{
			vips: map[string]bool{"vip-customer-1": true, "vip-customer-2": true},
		},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	customers := []string{"vip-customer-1", "regular-42", "vip-customer-2", "regular-7", "regular-42"}
	for i, c := range customers {
		msg := kafka.Message{
			Key:   []byte(c),
			Value: []byte(fmt.Sprintf(`{"seq":%d,"action":"purchase"}`, i)),
		}
		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Fatalf("write: %v", err)
		}
	}
	log.Println("sent VIP-routed messages — vip-customer-* always lands on partition 0")
}
