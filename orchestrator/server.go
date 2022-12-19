package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/rs/zerolog/log"
)

type TaskOrchestrationServer struct {
	rdb         *redis.Client
	lockHandler LockHandler
}

func (s *TaskOrchestrationServer) StartServer(ctx context.Context) {
	lockID, err := s.lockHandler.AcquireLock(ctx, "test", 5*time.Second, 10*time.Second)
	if err != nil {
		log.Error().Msgf("failed to acquire lock: %s", err)
		return
	}
	time.Sleep(3 * time.Second)
	s.lockHandler.ReleaseLock(ctx, "test", lockID)
	log.Info().Msg("successfully created and released lock")
	select {}
}

func NewTaskOrchestratorServer(cfg Config) *TaskOrchestrationServer {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisIP, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	return &TaskOrchestrationServer{
		rdb:         client,
		lockHandler: NewLockHandler(client),
	}
}
