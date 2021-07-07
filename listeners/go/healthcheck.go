package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	"time"
)

type HealthCheck struct {
	Status        int
	Active        bool
	ErrorMessages []string
}

var httpServer *http.Server

var tasksReceived = promauto.NewCounter(prometheus.CounterOpts{
	Name: "tasks_received_total",
	Help: "The total number of tasks received",
})

var tasksProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "tasks_processed_total",
	Help: "The total number of tasks processed successfully",
})

var tasksFailed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "tasks_failed_total",
	Help: "The total number of tasks failed",
})

var isActive = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "is_active",
	Help: "Instance Status Active/Standby",
})

func serveHealthCheck() {

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleRequest)
	mux.Handle("/metrics", promhttp.Handler())

	httpServer = &http.Server{
		Addr:        ":5000",
		Handler:     mux,
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}

	http.HandleFunc("/health", handleRequest)
	http.Handle("/metrics", promhttp.Handler())

	log.Info("Started Healthcheck Listener")
	err := httpServer.ListenAndServe()
	if err != nil {
		if err.Error() != "http: Server closed" {
			log.WithField("error", err).Error("Error Starting HealthCheck Listener")
		}
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
	c1 := make(chan string, 1)

	go func() {
		status, err := rdb.Ping(ctx).Result()
		if err != nil {
			log.WithFields(logrus.Fields{"error": err}).Error("Error while executing redis client ping command")
			outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisClient: %s", err.Error()))
		}
		c1 <- status
	}()

	select {
	case res := <-c1:
		fmt.Println(res)
	case <-time.After(1 * time.Second):
		log.Error("Timeout while executing redis client ping command")
		outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisClient: Ping Timeout"))
	}

}

func checkRedisLock(outputState *HealthCheck) {
	outputState.Active = lockAcquired
}

func checkRedisPubSub(outputState *HealthCheck) {
	c1 := make(chan string, 1)

	go func() {
		err := pubSub.Ping(ctx)
		if err != nil {
			log.WithFields(logrus.Fields{"error": err}).Error("Error while executing redis PubSub ping command")
			outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisPubSub: %s", err.Error()))
		}
		c1 <- ""
	}()

	select {
	case res := <-c1:
		fmt.Println(res)
	case <-time.After(1 * time.Second):
		log.Error("Timeout while executing redis PubSub ping command")
		outputState.ErrorMessages = append(outputState.ErrorMessages, fmt.Sprintf("RedisPubSub: Ping Timeout"))
	}

}
