package main

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"net/http"
)

type HealthCheck struct {
	Status        int
	Active        bool
	ErrorMessages []string
}

func serveHealthCheck() {
	http.HandleFunc("/health", handleRequest)
	log.Info("Started Healthcheck Listener")
	err := http.ListenAndServe(":5000", nil)
	if err != nil {
		log.WithField("error", err).Error("Error Starting HealthCheck Listener")
	}
}
func handleRequest(w http.ResponseWriter, _ *http.Request) {

	outputState := &HealthCheck{}
	checkRedisConnection(outputState)
	checkRedisLock(outputState)
	checkRedisPubSub(outputState)
	if len(outputState.ErrorMessages) > 0 {
		outputState.Status = http.StatusServiceUnavailable
	} else {
		outputState.Status = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(outputState.Status)
	err := json.NewEncoder(w).Encode(outputState)
	if err != nil {
		log.WithField("error", err).Error("Error handling healthcheck request")
	}
}

func checkRedisConnection(outputState *HealthCheck) {
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.WithFields(logrus.Fields{"error": err}).Error("Error while executing redis client ping command")
		outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisClient: %s", err.Error()))
	}
}

func checkRedisLock(outputState *HealthCheck) {
	outputState.Active = lockAcquired
}

func checkRedisPubSub(outputState *HealthCheck) {
	err := pubSub.Ping(ctx)
	if err != nil {
		log.WithFields(logrus.Fields{"error": err}).Error("Error while executing redis PubSub ping command")
		outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisPubSub: %s", err.Error()))
	}
}
