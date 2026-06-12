package gleanclient

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

type GleanPushClient struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
	appID      string
}

// NewGleanPushClient provisions an mTLS/TLS 1.3 hardened connector instance
func NewGleanPushClient() (*GleanPushClient, error) {
	instanceURL := os.getenv("GLEAN_INSTANCE_URL")
	apiKey := os.getenv("GLEAN_INDEXING_API_KEY")
	appID := os.getenv("GLEAN_APP_ID")

	if instanceURL == "" || apiKey == "" {
		return nil, fmt.Errorf("missing critical Glean connection profiles in environment config")
	}

	// Enforce strict TLS 1.3 transit encryption definitions
	tlsConfig := &tls.Config{
		MinVersion:           tls.VersionTLS13,
		PreferServerCipherSuites: true,
	}

	transport := &http.http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &GleanPushClient{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   15 * time.Second,
		},
		baseURL: instanceURL,
		apiKey:  apiKey,
		appID:   appID,
	}, nil
}

// PushDocument dispatches the payload to the Glean Indexing push endpoint
func (c *GleanPushClient) PushDocument(ctx context.Context, doc *DocumentDefinition) error {
	// Endpoint construction pattern: /api/v1/indexdocument
	endpoint := fmt.Sprintf("%s/api/v1/indexdocument", c.baseURL)

	payloadBytes, err json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to serialize Glean ingestion schema: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to construct HTTP request context: %w", err)
	}

	// Apply Authorization headers and define the tenant identity provider boundaries
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	req.Header.Set("X-Glean-App-ID", c.appID)
	req.Header.Set("X-Glean-Identity-Provider", "azure_ad")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("glean backend gateway transmission failure: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("glean API returned non-success execution status: %s", resp.Status)
	}

	return nil
}