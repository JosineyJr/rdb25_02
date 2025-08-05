package config

import (
	"os"
	"strconv"
)

var (
	PORT, PAYMENTS_PROCESSOR_URL_DEFAULT, PAYMENTS_PROCESSOR_URL_FALLBACK         string
	HEALTH_PROCESSOR_URL_DEFAULT, HEALTH_PROCESSOR_URL_FALLBACK                   string
	PAYMENT_PROCESSOR_TAX_DEFAULT, PAYMENT_PROCESSOR_TAX_FALLBACK                 float32
	NUM_WORKERS, NUM_PAYMENT_WORKERS, POOL_MAX_SIZE_FACTOR, POOL_INIT_SIZE_FACTOR int
)

func LoadEnv() {
	PORT = os.Getenv("PORT")
	if PORT == "" {
		PORT = ":80"
	}
	PAYMENTS_PROCESSOR_URL_DEFAULT = os.Getenv("PAYMENTS_PROCESSOR_URL_DEFAULT")
	PAYMENTS_PROCESSOR_URL_FALLBACK = os.Getenv("PAYMENTS_PROCESSOR_URL_FALLBACK")
	HEALTH_PROCESSOR_URL_DEFAULT = os.Getenv("HEALTH_PROCESSOR_URL_DEFAULT")
	HEALTH_PROCESSOR_URL_FALLBACK = os.Getenv("HEALTH_PROCESSOR_URL_FALLBACK")
	PAYMENT_PROCESSOR_TAX_DEFAULT = 0.05
	PAYMENT_PROCESSOR_TAX_FALLBACK = 0.15

	numWorkersStr := os.Getenv("NUM_WORKERS")
	var err error
	NUM_WORKERS, err = strconv.Atoi(numWorkersStr)
	if err != nil || NUM_WORKERS <= 0 {
		NUM_WORKERS = 10
	}

	numPaymentWorkersStr := os.Getenv("NUM_PAYMENT_WORKERS")
	NUM_PAYMENT_WORKERS, err = strconv.Atoi(numPaymentWorkersStr)
	if err != nil || NUM_PAYMENT_WORKERS <= 0 {
		NUM_PAYMENT_WORKERS = 5
	}

	poolMaxSizeFactorStr := os.Getenv("POOL_MAX_SIZE_FACTOR")
	POOL_MAX_SIZE_FACTOR, err = strconv.Atoi(poolMaxSizeFactorStr)
	if err != nil || POOL_MAX_SIZE_FACTOR <= 0 {
		POOL_MAX_SIZE_FACTOR = 5
	}

	poolInitSizeFactorStr := os.Getenv("POOL_INIT_SIZE_FACTOR")
	POOL_INIT_SIZE_FACTOR, err = strconv.Atoi(poolInitSizeFactorStr)
	if err != nil || POOL_INIT_SIZE_FACTOR <= 0 {
		POOL_INIT_SIZE_FACTOR = 2
	}
}
