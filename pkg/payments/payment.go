package payments

import (
	"time"
)

var (
	DefaultProcessor  string = "default"
	FallbackProcessor string = "fallback"
)

type PaymentsPayload struct {
	RequestedAt   time.Time `json:"requestedAt"`
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
}

type SummaryData struct {
	Count int64   `json:"totalRequests"`
	Total float64 `json:"totalAmount"`
}

type PaymentsSummary struct {
	Default  SummaryData `json:"default"`
	Fallback SummaryData `json:"fallback"`
}

type ServiceHealthPayload struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}
