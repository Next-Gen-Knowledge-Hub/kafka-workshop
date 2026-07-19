// Command topicadmin exercises the Kafka Admin API from Go: create a topic with
// explicit config, list/describe it, inspect its dynamic config, and describe a
// consumer group — the same operations the kafka-topics/kafka-configs/
// kafka-consumer-groups CLI tools issue under the hood.
//
// Point it at the 3-broker cluster from docker-compose.cluster.yml:
//
//	go run . localhost:9093,localhost:9094,localhost:9095
//
// or at the single-broker cluster from 1-setup:
//
//	go run . localhost:9092
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "admin-demo-topic"

func main() {
	brokers := []string{"localhost:9092"}
	if len(os.Args) > 1 {
		brokers = strings.Split(os.Args[1], ",")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createTopic(ctx, admin)
	listAndDescribeTopic(ctx, admin)
	describeConfig(ctx, admin)
	alterConfig(ctx, admin)
	listGroups(ctx, admin)
}

func createTopic(ctx context.Context, admin *kadm.Client) {
	resp, err := admin.CreateTopics(ctx, 3, 1, map[string]*string{
		"retention.ms":        kadm.StringPtr("3600000"), // 1 hour, deliberately short for the demo
		"cleanup.policy":      kadm.StringPtr("delete"),
		"min.insync.replicas": kadm.StringPtr("1"),
	}, topic)
	if err != nil {
		log.Fatalf("create topics: %v", err)
	}
	for _, r := range resp {
		if r.Err != nil && !strings.Contains(r.Err.Error(), "TOPIC_ALREADY_EXISTS") {
			log.Fatalf("create topic %q: %v (%s)", r.Topic, r.Err, r.ErrMessage)
		}
		log.Printf("topic %q ready: partitions=%d replicationFactor=%d", r.Topic, r.NumPartitions, r.ReplicationFactor)
	}
}

func listAndDescribeTopic(ctx context.Context, admin *kadm.Client) {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		log.Fatalf("list topics: %v", err)
	}
	td, ok := details[topic]
	if !ok || td.Err != nil {
		log.Fatalf("describe topic %q failed: %v", topic, td.Err)
	}
	fmt.Printf("\n=== %s ===\n", topic)
	for _, p := range td.Partitions.Sorted() {
		fmt.Printf("  partition=%d leader=%d replicas=%v isr=%v\n", p.Partition, p.Leader, p.Replicas, p.ISR)
	}
}

func describeConfig(ctx context.Context, admin *kadm.Client) {
	configs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		log.Fatalf("describe configs: %v", err)
	}
	rc, err := configs.On(topic, nil)
	if err != nil {
		log.Fatalf("config for %q: %v", topic, err)
	}
	fmt.Printf("\n=== %s config (non-default only) ===\n", topic)
	for _, c := range rc.Configs {
		if c.Source == kmsg.ConfigSourceDefaultConfig {
			continue
		}
		val := "<sensitive>"
		if c.Value != nil {
			val = *c.Value
		}
		fmt.Printf("  %-30s = %-15s (source=%s)\n", c.Key, val, c.Source)
	}
}

func alterConfig(ctx context.Context, admin *kadm.Client) {
	_, err := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "retention.ms", Value: kadm.StringPtr("7200000")},
	}, topic)
	if err != nil {
		log.Fatalf("alter config: %v", err)
	}
	log.Println("bumped retention.ms to 2h without restarting a single broker")
}

func listGroups(ctx context.Context, admin *kadm.Client) {
	groups, err := admin.ListGroups(ctx)
	if err != nil {
		log.Fatalf("list groups: %v", err)
	}
	fmt.Println("\n=== consumer groups on this cluster ===")
	if len(groups) == 0 {
		fmt.Println("  (none yet — run stage 2's consumergroup example first)")
		return
	}
	for _, g := range groups.Sorted() {
		fmt.Printf("  %-30s state=%s\n", g.Group, g.State)
	}
}
