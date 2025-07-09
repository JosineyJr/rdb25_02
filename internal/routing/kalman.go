package routing

import (
	"sync"
	"time"
)

type State struct {
	Latency   float64
	ErrorRate float64
}

type KalmanFilter struct {
	mu       State
	sigma    [2][2]float64
	muMutex  sync.Mutex
	Q        [2][2]float64
	R        [2][2]float64
	lastTime time.Time
}

func NewKalmanFilter(initial State, initCov, processNoise, measNoise [2][2]float64) *KalmanFilter {
	return &KalmanFilter{
		mu:       initial,
		sigma:    initCov,
		Q:        processNoise,
		R:        measNoise,
		lastTime: time.Now(),
	}
}

func (kf *KalmanFilter) Predict() {
	kf.muMutex.Lock()
	defer kf.muMutex.Unlock()
	dt := time.Since(kf.lastTime).Seconds()
	kf.lastTime = time.Now()
	kf.sigma[0][0] += kf.Q[0][0] * dt
	kf.sigma[0][1] += kf.Q[0][1] * dt
	kf.sigma[1][0] += kf.Q[1][0] * dt
	kf.sigma[1][1] += kf.Q[1][1] * dt
}

func (kf *KalmanFilter) Update(meas State, measCov [2][2]float64) {
	kf.muMutex.Lock()
	defer kf.muMutex.Unlock()

	S := [2][2]float64{
		{kf.sigma[0][0] + measCov[0][0], kf.sigma[0][1] + measCov[0][1]},
		{kf.sigma[1][0] + measCov[1][0], kf.sigma[1][1] + measCov[1][1]},
	}
	det := S[0][0]*S[1][1] - S[0][1]*S[1][0]
	if det == 0 {
		return
	}
	inv := [2][2]float64{
		{S[1][1] / det, -S[0][1] / det},
		{-S[1][0] / det, S[0][0] / det},
	}
	K := [2][2]float64{
		{
			kf.sigma[0][0]*inv[0][0] + kf.sigma[0][1]*inv[1][0],
			kf.sigma[0][0]*inv[0][1] + kf.sigma[0][1]*inv[1][1],
		},
		{
			kf.sigma[1][0]*inv[0][0] + kf.sigma[1][1]*inv[1][0],
			kf.sigma[1][0]*inv[0][1] + kf.sigma[1][1]*inv[1][1],
		},
	}

	residualLatency := meas.Latency - kf.mu.Latency
	residualError := meas.ErrorRate - kf.mu.ErrorRate
	kf.mu.Latency += K[0][0]*residualLatency + K[0][1]*residualError
	kf.mu.ErrorRate += K[1][0]*residualLatency + K[1][1]*residualError

	IminusK := [2][2]float64{
		{1 - K[0][0], -K[0][1]},
		{-K[1][0], 1 - K[1][1]},
	}
	kf.sigma[0][0] = IminusK[0][0]*kf.sigma[0][0] + IminusK[0][1]*kf.sigma[1][0]
	kf.sigma[0][1] = IminusK[0][0]*kf.sigma[0][1] + IminusK[0][1]*kf.sigma[1][1]
	kf.sigma[1][0] = IminusK[1][0]*kf.sigma[0][0] + IminusK[1][1]*kf.sigma[1][0]
	kf.sigma[1][1] = IminusK[1][0]*kf.sigma[0][1] + IminusK[1][1]*kf.sigma[1][1]
}

func (kf *KalmanFilter) Estimate() State {
	kf.muMutex.Lock()
	defer kf.muMutex.Unlock()
	return kf.mu
}

func (kf *KalmanFilter) FeedMeasurement(getHealth func() State) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			kf.Predict()

			kf.Update(getHealth(), kf.R)
		}
	}()
}
