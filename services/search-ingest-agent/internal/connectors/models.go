package connectors

import "time"

// IngestPayload is the conformed structure our Go agent passes to Confluent Cloud
type IngestPayload struct {
	SourceSystem string    `json:"source_system"`
	EntityID     string    `json:"entity_id"`
	RawPayload   string    `json:"raw_payload"`
	ACLMetadata  []string  `json:"acl_metadata"`
	EventTS      time.Time `json:"event_ts"`
}

// SlackWebhook represents the raw structural footprint hitting our endpoint from Slack
type SlackWebhook struct {
	ChannelID string   `json:"channel"`
	UserID    string   `json:"user"`
	Text      string   `json:"text"`
	TeamID    string   `json:"team_id"`
	AuthedFor []string `json:"authed_users"` // Mapped to raw ACL baselines
}

// SalesforceCDC represents the payload coming from Salesforce Pub/Sub or Streaming APIs
type SalesforceCDC struct {
	ChangeEventHeader struct {
		EntityName string   `json:"entityName"`
		RecordIds  []string `json:"recordIds"`
	} `json:"changeEventHeader"`
	OwnerID string `json:"OwnerId"`
	Payload string `json:"payload_fields"`
}