package algoliaclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// AlgoliaSearchRecord represents a lean, high-performance UI search index entity
type AlgoliaSearchRecord struct {
	ObjectID         string    `json:"objectID"` // Algolia's mandatory primary key string
	Title            string    `json:"title"`
	SourcePlatform   string    `json:"source_platform"`
	MetadataCategory string    `json:"metadata_category"`
	PermittedGroups  []string  `json:"permitted_groups"` // Mapped Azure AD Object IDs for edge filtering
	LastUpdatedUnix  int64     `json:"last_updated_unix"`
	URL              string    `json:"url,omitempty"`
}

type AlgoliaPushClient struct {
	httpClient *http.Client
	appID      string
	writeKey   string
}

// NewAlgoliaPushClient provisions an edge-optimized REST ingestion connector
func NewAlgoliaPushClient() (*AlgoliaPushClient, error) {
	appID := os.Getenv("ALGOLIA_APPLICATION_ID")
	writeKey := os.Getenv("ALGOLIA_WRITE_API_KEY")

	if appID == "" || writeKey == "" {
		return nil, fmt.Errorf("missing critical Algolia API application registration profiles")
	}

	// Enforce strict TLS 1.3 transit encryption
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	transport := &http.Transport{TLSClientConfig: tlsConfig}

	return &AlgoliaPushClient{
		httpClient: &http.Client{Transport: transport, Timeout: 8 * time.Second},
		appID:      appID,
		writeKey:   writeKey,
	}, nil
}

// SaveRecord publishes a lean metadata event directly to an Algolia index partition
func (c *AlgoliaPushClient) SaveRecord(ctx context.Context, indexName string, record *AlgoliaSearchRecord) error {
	// Constructing standard Algolia REST Endpoint: /1/indexes/{indexName}/{objectID}
	endpoint := fmt.Sprintf("https://%s-dsn.algolia.net/1/indexes/%s/%s", c.appID, indexName, record.ObjectID)

	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed serialization of lean Algolia UI envelope: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to build Algolia edge request framework: %w", err)
	}

	// Apply Algolia custom header authentication protocol
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Algolia-Application-Id", c.appID)
	req.Header.Set("X-Algolia-API-Key", c.writeKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("algolia edge network communication timeout: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("algolia edge node rejected push, status code: %s", resp.Status)
	}

	return nil
}