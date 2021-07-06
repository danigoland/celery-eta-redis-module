package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TODO: Add Datadog Tracer

var log = logrus.New()
var rdb *redis.Client
var redisLockClient *redsync.Redsync
var redisMutex *redsync.Mutex
var lockExtenderChannel chan struct{}
var ctx = context.Background()
var pubSub *redis.PubSub
var wg sync.WaitGroup
var lockAcquired bool

const (
	pubSubChannelSize = 1000
	redisMutexName    = "celery-eta-scheduler-mutex"
	etaPrefix         = "eta:"
)

func init() {
	lockAcquired = false
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
	redisLockPool := goredis.NewPool(rdb)
	redisLockClient = redsync.New(redisLockPool)
	redisMutex = redisLockClient.NewMutex(redisMutexName, redsync.WithRetryDelay(1*time.Second),
		redsync.WithTries(1))
}

func performETA(keyName string) {
	defer wg.Done()
	parts := strings.Split(strings.TrimPrefix(keyName, etaPrefix), ":")
	queue := parts[0]
	taskId := parts[1]
	taskLogger := log.WithFields(logrus.Fields{"queue": queue, "task_id": taskId})

	payload, err := rdb.HGet(ctx, fmt.Sprintf("data:%s", queue), taskId).Result()

	switch {
	case err == redis.Nil:
		taskLogger.WithField("error", err).Error("Key does not exist, Task already executed")
		return
	case err != nil:
		taskLogger.WithField("error", err).Error("Error getting the task payload from the hashset")
		return
	case payload == "":
		taskLogger.Error("HGET payload is empty")
		return
	}
	_, err = rdb.RPush(ctx, queue, payload).Result()
	if err != nil {
		taskLogger.WithField("error", err).Error("Error pushing the task payload to the queue/list")
		return
	}
	_, err = rdb.HDel(ctx, fmt.Sprintf("data:%s", queue), taskId).Result()
	if err != nil {
		taskLogger.WithField("error", err).Error("Error deleting the task payload from the hashset")
		return
	}
	taskLogger.Info("Successfully processed ETA task")
}

func listen(ch <-chan *redis.Message) {
	for msg := range ch {
		if strings.Contains(msg.Payload, etaPrefix) {
			wg.Add(1)
			go performETA(msg.Payload)
		}

	}
}

func cleanup(ch <-chan *redis.Message) {
	// Close the PubSub Channel
	err := pubSub.Close()
	if err != nil {
		if err != redis.ErrClosed {
			panic(err)
		}
	}
	log.Info("PubSub Subscription Closed")

	// Release the lock so other instances can start processing tasks
	status, err := redisMutex.UnlockContext(ctx)
	if err != nil {
		panic(err)
	}
	if status {
		log.Info("Redis Lock Released")
		lockAcquired = false
	}
	listen(ch)
	wg.Wait()
	os.Exit(0)
}

func main() {

	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})
	// TODO: Only set config if doesn't exist
	rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA")

	// Even if another instance has a lock, we still collect the most recent 1000 messages to use if a fail-over occurs
	pubSub = rdb.Subscribe(ctx, "__keyevent@0__:expired")
	// Wait for confirmation that subscription is created
	_, err := pubSub.Receive(ctx)
	if err != nil {
		log.WithField("error", err).Error("Error receiving subscription confirmation")
		panic(err)
	}

	ch := pubSub.ChannelSize(pubSubChannelSize)

	// OS Signal Handling
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	//	<- sigc
	go func() {
		<-sigC // Blocks here until interrupted
		log.Info("SIGTERM received. Shutdown process initiated\n")
		cleanup(ch)
	}()

	go serveHealthCheck()

	defer cleanup(ch)

	for {
		if err := redisMutex.LockContext(ctx); err != nil {
			if err == redsync.ErrFailed {
				// If we don't have a lock we only keep the last 500 messages in the queue
				if len(ch) > pubSubChannelSize/2 {
					for i := 0; i < pubSubChannelSize/10; i++ {
						<-ch
					}
				}
				lockAcquired = false
				continue
			}
			panic(err)
		}
		log.Info("Obtained a lock!")
		lockAcquired = true
		ticker := time.NewTicker(4 * time.Second)
		lockExtenderChannel = make(chan struct{})
		go func() {
			for {
				select {
				case <-ticker.C:
					extended, err := redisMutex.ExtendContext(ctx)
					if err != nil {
						close(lockExtenderChannel)
						lockAcquired = false
					}
					if !extended {
						close(lockExtenderChannel)
						lockAcquired = false
					}
					lockAcquired = true
				case <-lockExtenderChannel:
					// This will currently cause the program to die and relies on an orchestrator to restart it
					// Implement a retry loop
					lockAcquired = false
					ticker.Stop()
					cleanup(ch)
					return
				}
			}
		}()
		listen(ch)

	}

}
