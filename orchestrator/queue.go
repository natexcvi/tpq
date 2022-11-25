package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/natexcvi/tpq/common"
)

type QueueHandler interface {
	ExecuteTopTask(ctx context.Context) error
	MoveDueDelayedTasks(ctx context.Context, dueBefore time.Time) error
}

type queueHandler struct {
	rdb         *redis.Client
	lockHandler LockHandler
	queueID     string
}

func (h *queueHandler) MoveDueDelayedTasks(ctx context.Context, dueBefore time.Time) error {
	queueName := common.QueuePrefix + ":" + h.queueID
	delayedQueueName := common.DelayedQueuePrefix + ":" + h.queueID
	delayedQueueLock, err := h.lockHandler.AcquireLock(ctx, delayedQueueName, 3*time.Second, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed to acquire delayed queue lock: %w", err)
	}
	defer h.lockHandler.ReleaseLock(ctx, delayedQueueName, delayedQueueLock)
	err = h.rdb.Watch(ctx, func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			delayedTasks, err := p.ZRangeByScore(ctx, delayedQueueName, &redis.ZRangeBy{
				Min: "-inf",
				Max: fmt.Sprintf("%d", dueBefore.Unix()),
			}).Result()
			if err != nil {
				return fmt.Errorf("failed to get due tasks: %w", err)
			}
			tasks := make([]redis.Z, len(delayedTasks))
			for _, task := range delayedTasks {
				parsedTask, err := common.ParseTask(task)
				if err != nil {
					return err
				}
				encodedTask, err := parsedTask.EncodeJSON()
				if err != nil {
					return err
				}
				tasks = append(tasks, redis.Z{
					Score:  parsedTask.Priority,
					Member: encodedTask,
				})
			}
			_, err = p.ZAdd(ctx, queueName, tasks...).Result()
			if err != nil {
				return fmt.Errorf("failed to add tasks to main queue: %w", err)
			}
			return nil
		})
		return err
	}, delayedQueueName)
	if err != nil {
		return fmt.Errorf("failed to move tasks: %w", err)
	}
	return nil
}

func (h *queueHandler) ExecuteTopTask(ctx context.Context) error {
	queueName := common.QueuePrefix + ":" + h.queueID
	streamName := common.StreamPrefix + ":" + h.queueID
	lock, err := h.lockHandler.AcquireLock(ctx, queueName, 3*time.Second, 200*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to acquire queue lock: %w", err)
	}
	topTasks, err := h.rdb.ZRange(ctx, queueName, 0, 0).Result()
	if err != nil {
		return fmt.Errorf("failed to get top task: %w", err)
	}
	if len(topTasks) == 0 {
		return nil
	}

	task, err := common.ParseTask(topTasks[0])
	if err != nil {
		return err
	}

	_, err = h.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"task": task,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to send task to workers: %w", err)
	}

	_, err = h.rdb.ZPopMax(ctx, queueName, 1).Result()
	if err != nil {
		return fmt.Errorf("failed to pop task sent to workers: %w", err)
	}
	err = h.lockHandler.ReleaseLock(ctx, queueName, lock)
	if err != nil {
		return fmt.Errorf("failed to release queue lock: %w", err)
	}
	return nil
}
