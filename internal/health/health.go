package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

type HealthUpdater struct {
	router *routing.AdaptiveRouter
	client *http.Client
	ticker *time.Ticker
	done   chan bool
}

func NewHealthUpdater(router *routing.AdaptiveRouter) *HealthUpdater {
	return &HealthUpdater{
		router: router,
		client: &http.Client{},
		ticker: time.NewTicker(7000 * time.Millisecond),
		done:   make(chan bool),
	}
}

func (hu *HealthUpdater) Start(ctx context.Context) {
	hu.updateMetrics()

	go func() {
		for {
			select {
			case <-ctx.Done():
				hu.ticker.Stop()
				return
			case <-hu.ticker.C:
				hu.updateMetrics()
				hu.ticker.Reset(7000 * time.Millisecond)
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

	defaultLatencySeconds := float64(defaultState.MinResponseTime) / 1000.0
	fallbackLatencySeconds := float64(fallbackState.MinResponseTime) / 1000.0

	hu.router.UpdateHealthMetrics(
		defaultLatencySeconds,
		fallbackLatencySeconds,
		defaultState.Failing,
	)
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
