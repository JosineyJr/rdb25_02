package health

import (
	"encoding/json"
	"net/http"

	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

func GetHealthStatus(client *http.Client, healthURL string) routing.State {
	resp, err := client.Get(healthURL)
	if err != nil {
		return routing.State{Latency: 5.0, ErrorRate: 1.0}
	}
	defer resp.Body.Close()

	var healthPayload payments.ServiceHealthPayload
	if err := json.NewDecoder(resp.Body).Decode(&healthPayload); err != nil {
		return routing.State{Latency: 5.0, ErrorRate: 1.0}
	}

	var state routing.State
	if healthPayload.Failing {
		state.ErrorRate = 1.0
	} else {
		state.ErrorRate = 0.0
	}

	state.Latency = float64(healthPayload.MinResponseTime) / 1000.0

	return state
}
