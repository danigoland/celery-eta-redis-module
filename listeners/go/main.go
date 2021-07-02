package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

var log = logrus.New()
var rdb *redis.Client
var ctx = context.Background()

func init() {
	redisURI := os.Getenv("REDIS_URI")
	if redisURI == "" {
		redisURI = "redis://localhost:6379/0"
	}


	options, err := redis.ParseURL(redisURI)
	if err != nil {
		log.WithFields(logrus.Fields{"error": err, "redis_uri": redisURI}).Error("Error parsing redis uri")
		panic(err)
	}
	options.DialTimeout = 10 * time.Second
	options.ReadTimeout = 30 * time.Second
	options.WriteTimeout = 30 * time.Second
	options.PoolSize = 20
	options.PoolTimeout = 30 * time.Second

	rdb = redis.NewClient(options)
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.WithFields(logrus.Fields{"error": err, "redis_uri": redisURI}).Error("Error while executing ping command")
		panic(err)
	}
	log.WithField("redis_uri", redisURI).Info("Ping response: ", pong)
}

func performETA(keyName string){
	parts := strings.Split(keyName, ":")
	queue := parts[0]
	taskId := parts[1]
	taskLogger := log.WithFields(logrus.Fields{"queue": queue, "task_id": taskId})

	payload, err := rdb.HGet(ctx, fmt.Sprintf("data:%s", queue), taskId).Result()
	if err != nil {
		taskLogger.WithField("error", err).Error("Error getting the task payload from the hashset")
		return
	}
	_, err = rdb.HDel(ctx, fmt.Sprintf("data:%s", queue), taskId).Result()
	if err != nil {
		taskLogger.WithField("error", err).Error("Error deleting the task payload from the hashset")
		return
	}
	_, err = rdb.RPush(ctx, queue, payload).Result()
	if err != nil {
		taskLogger.WithField("error", err).Error("Error pushing the task payload to the queue/list")
		return
	}
	taskLogger.Info("Successfully processed ETA task")
}

func main() {
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: time.RFC3339Nano,
	})

	rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA")
	pubsub := rdb.Subscribe(ctx, "__keyevent@0__:expired")

	defer pubsub.Close()

	// Wait until subscription is confirmed
	_, err := pubsub.Receive(ctx)
	if err != nil {
		log.WithField("error", err).Error("Error receiving subscription confirmation")
		panic(err)
	}

	ch := pubsub.Channel()

	for msg := range ch {
		if strings.Contains(msg.Payload, ":") {
			go performETA(msg.Payload)
		}

	}


}