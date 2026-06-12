package coveoclient

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

// CoveoPushDocument matches Coveo's metadata catalog layout
type CoveoPushDocument struct {
	DocumentID       string                 `json:"documentId"` // The primary URI matching the source object
	Title            string                 `json:"title"`
	Content          string                 `json:"content"`
	SourcePlatform   string                 `json:"source_platform"`
	MetadataCategory string                 `json:"metadata_category"`
	CompressedVector []float32              `json:"compressed_vector,omitempty"`
	FileExtension    string                 `json:"fileExtension"`
	Permissions      []string               `json:"permissions"` // Maps directly to our Azure AD identifiers
	CustomFields     map[string]interface{} `json:"customFields,omitempty"`
}

type CoveoPushClient struct {
	httpClient *http.Client
	orgID      string
	apiKey     string
}

func NewCoveoPushClient() (*CoveoPushClient, error) {
	orgID := os.Getenv("COVEO_ORGANIZATION_ID")
	apiKey := os.Getenv("COVEO_PUSH_API_KEY")

	if orgID == "" || apiKey == "" {
		return nil, fmt.Errorf("missing critical Coveo organization profile records")
	}

	// Enforce strict TLS 1.3 transit requirements
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	transport := &http.Transport{TLSClientConfig: tlsConfig}

	return &CoveoPushClient{
		httpClient: &http.Client{Transport: transport, Timeout: 10 * time.Second},
		orgID:      orgID,
		apiKey:     apiKey,
	}, nil
}

// PushToCoveo dispatches the document into Coveo's Push API catalog source
func (c *CoveoPushClient) PushToCoveo(ctx context.Context, sourceID string, doc *CoveoPushDocument) error {
	// Constructing standard Coveo Push API endpoint: /rest/v1/organizations/{orgId}/sources/{sourceId}/documents
	endpoint := fmt.Sprintf("https://push.cloud.coveo.com/v1/organizations/%s/sources/%s/documents?documentId=%s",
		c.orgID, sourceID, doc.DocumentID)

	payloadBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed serialization of Coveo data schema: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed setting up Coveo HTTP frame: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("coveo pipeline delivery communication timeout: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("coveo gateway returned non-success code: %s", resp.Status)
	}

	return nil
}