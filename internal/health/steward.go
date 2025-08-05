package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

type HealthReport struct {
	Default  payments.ServiceHealthPayload `json:"default"`
	Fallback payments.ServiceHealthPayload `json:"fallback"`
}

type HealthUpdater struct {
	client  *http.Client
	ticker  *time.Ticker
	updates chan<- HealthReport
}

func NewHealthUpdater(updates chan<- HealthReport) *HealthUpdater {
	return &HealthUpdater{
		client:  &http.Client{},
		ticker:  time.NewTicker(5 * time.Second),
		updates: updates,
	}
}

func (hu *HealthUpdater) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				hu.ticker.Stop()
				return
			case <-hu.ticker.C:
				hu.updateMetrics()
				hu.ticker.Reset(5 * time.Second)
			}
		}
	}()
}

func (hu *HealthUpdater) updateMetrics() {
	var wg sync.WaitGroup
	var defaultState, fallbackState payments.ServiceHealthPayload

	wg.Add(2)

	go func() {
		defer wg.Done()
		defaultState = hu.getHealthStatus(config.HEALTH_PROCESSOR_URL_DEFAULT)
	}()

	go func() {
		defer wg.Done()
		fallbackState = hu.getHealthStatus(config.HEALTH_PROCESSOR_URL_FALLBACK)
	}()

	wg.Wait()

	hu.updates <- HealthReport{
		Default:  defaultState,
		Fallback: fallbackState,
	}
}

func (hu *HealthUpdater) getHealthStatus(healthURL string) payments.ServiceHealthPayload {
	defaultPayload := payments.ServiceHealthPayload{Failing: true, MinResponseTime: 99999}

	req, err := http.NewRequest(http.MethodGet, healthURL, nil)
	if err != nil {
		return defaultPayload
	}

	resp, err := hu.client.Do(req)
	if err != nil {
		return defaultPayload
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return defaultPayload
	}

	var healthPayload payments.ServiceHealthPayload
	if err := json.NewDecoder(resp.Body).Decode(&healthPayload); err != nil {
		return defaultPayload
	}

	return healthPayload
}
