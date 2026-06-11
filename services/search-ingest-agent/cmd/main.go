package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"services/search-ingest-agent/internal/connectors"
)

type IngestAgentServer struct {
	producer *kafka.Producer
	topic    string
}

func main() {
	log.Println("Initializing MIA Ingest Webhook Agent...")

	// 1. Fetch Secure Network & Confluent Configurations from .env
	bootstrapServers := os.Getenv("CONFLUENT_BOOTSTRAP_SERVERS")
	topicName := "unified-enterprise-mutations"

	// Configure secure Kafka connection parameters matching our mTLS and SSL requirements
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"security.protocol":       "ssl",
		"ssl.enabled.protocols":   "TLSv1.3",
		"ssl.truststore.location": os.Getenv("SSL_TRUSTSTORE_LOCATION"),
		"ssl.truststore.password": os.Getenv("SSL_TRUSTSTORE_PASSWORD"),
		"ssl.keystore.location":   os.Getenv("SSL_KEYSTORE_LOCATION"),
		"ssl.keystore.password":   os.Getenv("SSL_KEYSTORE_PASSWORD"),
		"ssl.key.password":        os.Getenv("SSL_KEY_PASSWORD"),
		"acks":                    "all", // Ensure ironclad delivery compliance
		"compression.type":        "snappy",
	}

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to initialize Confluent Cloud Producer: %v", err)
	}
	defer producer.Close()

	server := &IngestAgentServer{
		producer: producer,
		topic:    topicName,
	}

	// Track Kafka Delivery Reports asynchronously in the background
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed for event partition: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("Successfully brokered event to partition %v at offset %v\n",
						ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// 2. Wire Up Routes and Middleware Controls
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/ingest/slack", server.HandleSlackWebhook)
	mux.HandleFunc("/v1/ingest/salesforce", server.HandleSalesforceCDC)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 3. Graceful Shutdown Framework
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen and Serve network failure: %v", err)
		}
	}()
	log.Println("Ingest Agent successfully listening on port :8080 with mTLS boundaries active.")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Ingest Agent gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown prematurely: %v", err)
	}
	producer.Flush(15 * 1000) // Ensure all pending streams drop cleanly onto brokers
	log.Println("Agent safely disconnected.")
}

// HandleSlackWebhook processes incoming un-conformed payloads from Slack Webhooks
func (s *IngestAgentServer) HandleSlackWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var rawSlack connectors.SlackWebhook
	if err := json.NewDecoder(r.Body).Decode(&rawSlack); err != nil {
		http.Error(w, "Malformed json payload", http.StatusBadRequest)
		return
	}

	// Normalize data on the fly to our unified Data Nervous System layout
	conformedEvent := connectors.IngestPayload{
		SourceSystem: "slack",
		EntityID:     rawSlack.ChannelID,
		RawPayload:   rawSlack.Text,
		ACLMetadata:  rawSlack.AuthedFor, // Capture initial ACL tags
		EventTS:      time.Now().UTC(),
	}

	s.dispatchToKafka(w, conformedEvent)
}

// HandleSalesforceCDC processes streaming Salesforce change event notifications
func (s *IngestAgentServer) HandleSalesforceCDC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cdcEvent connectors.SalesforceCDC
	if err := json.NewDecoder(r.Body).Decode(&cdcEvent); err != nil {
		http.Error(w, "Malformed JSON schema", http.StatusBadRequest)
		return
	}

	conformedEvent := connectors.IngestPayload{
		SourceSystem: "salesforce",
		EntityID:     cdcEvent.ChangeEventHeader.RecordIds[0],
		RawPayload:   cdcEvent.Payload,
		ACLMetadata:  []string{cdcEvent.OwnerID}, // Seed tracking using owner permission tags
		EventTS:      time.Now().UTC(),
	}

	s.dispatchToKafka(w, conformedEvent)
}

// Helper block to dispatch normalized events to Confluent Cloud Topics
func (s *IngestAgentServer) dispatchToKafka(w http.ResponseWriter, payload connectors.IngestPayload) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Internal serialization failure", http.StatusInternalServerError)
		return
	}

	err = s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
		Key:            []byte(fmt.Sprintf("%s:%s", payload.SourceSystem, payload.EntityID)),
		Value:          bytes,
	}, nil)

	if err != nil {
		http.Error(w, "Broker rejected message ingestion", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"queued","message":"Normalized event brokered to Confluent Cloud"}`))
}