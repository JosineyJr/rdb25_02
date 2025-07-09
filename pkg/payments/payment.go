package payments

import (
	"bytes"
	"time"
)

var (
	DefaultProcessor  string = "default"
	FallbackProcessor string = "fallback"
)

type PaymentsPayload struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type ConsolidatePayment struct {
	Tag          string
	Amount       float64
	ProcessorURL string
	Payload      *bytes.Reader
	RequestedAt  time.Time
}

type ServiceHealthPayload struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}
