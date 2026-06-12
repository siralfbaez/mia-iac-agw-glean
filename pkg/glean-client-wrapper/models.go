package gleanclient

import "time"

// DocumentDefinition represents the core object structure for Glean Ingestion
type DocumentDefinition struct {
	ID          string             `json:"id"`
	Title       string             `json:"title"`
	Body        ContentContainer   `json:"body"`
	UpdatedAt   int64              `json:"updatedAt"` // Unix timestamp format
	Permissions *GleanPermissions  `json:"permissions,omitempty"`
	Metadata    *CustomMetadata    `json:"metadata,omitempty"`
}

type ContentContainer struct {
	MimeType string `json:"mimeType"` // e.g., "text/plain", "text/html"
	TextContent string `json:"textContent"`
}

// GleanPermissions maps local identities straight to Azure AD Security Group Object IDs
type GleanPermissions struct {
	AllowedGroups []string `json:"allowedGroups"` // Array of Azure AD Group GUIDs
	DeniedGroups  []string `json:"deniedGroups,omitempty"`
}

// CustomMetadata holds our Vertex AI enriched data signals for search relevancy
type CustomMetadata struct {
	VectorEmbedding []float32 `json:"vectorEmbedding,omitempty"`
	AIClassification string    `json:"aiClassification,omitempty"`
	SourcePlatform   string    `json:"sourcePlatform"`
}