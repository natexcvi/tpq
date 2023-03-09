package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/hashicorp/go-multierror"
	"github.com/natexcvi/tpq/common"
)

type QueueClient interface {
	PushTask(ctx context.Context, task *common.Task) error
	TakeTask(ctx context.Context, taskHandler func(*common.Task) error) error
	TakePendingTasks(ctx context.Context, taskHandler func(*common.Task) error) error
	TakeNewTasks(ctx context.Context, count int, taskHandler func(*common.Task) error) error
	ClaimDanglingTasks(ctx context.Context, minIdleTime time.Duration) (next string, err error)
}

type queueClient struct {
	rdb      *redis.Client
	queueID  string
	workerID string
}

func (c *queueClient) pushDelayedTask(ctx context.Context, task *common.Task) error {
	delayedQueueName := common.DelayedQueuePrefix + ":" + c.queueID
	return c.pushTask(ctx, delayedQueueName, func(t *common.Task) float64 {
		return float64(t.ExecuteAfter.Unix())
	}, task)
}

func (c *queueClient) pushTask(ctx context.Context, queueName string, scoreExtractor func(*common.Task) float64, task *common.Task) error {
	encodedTask, err := task.EncodeJSON()
	if err != nil {
		return fmt.Errorf("failed to push task: %w", err)
	}
	_, err = c.rdb.ZAdd(ctx, queueName, redis.Z{
		Score:  scoreExtractor(task),
		Member: encodedTask,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to push task: %w", err)
	}
	return nil
}

func (c *queueClient) PushTask(ctx context.Context, task *common.Task) (err error) {
	if task.ExecuteAfter.After(time.Now()) {
		err = c.pushDelayedTask(ctx, task)
	}
	queueName := common.QueuePrefix + ":" + c.queueID
	err = c.pushTask(ctx, queueName, func(t *common.Task) float64 { return t.Priority }, task)
	return err
}

func (c *queueClient) takeTasks(ctx context.Context, tasks []redis.XMessage, taskHandler func(*common.Task) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(tasks))
	for _, rawTask := range tasks {
		task, err := common.ParseTask(rawTask.Values["task"].(string))
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- taskHandler(task)
		}()
	}
	wg.Wait()
	var multiErr *multierror.Error
	for err := range errs {
		multiErr = multierror.Append(multiErr, err)
	}
	return multiErr.ErrorOrNil()
}

func (c *queueClient) TakePendingTasks(ctx context.Context, taskHandler func(*common.Task) error) error {
	streamName := common.StreamPrefix + ":" + c.queueID
	res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    common.GroupPrefix + ":" + c.queueID,
		Consumer: c.workerID,
		Streams:  []string{streamName, "0"},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get pending tasks: %w", err)
	}
	return c.takeTasks(ctx, res[0].Messages, taskHandler)
}

func (c *queueClient) ClaimDanglingTasks(ctx context.Context, minIdleTime time.Duration) (next string, err error) {
	streamName := common.StreamPrefix + ":" + c.queueID
	_, next, err = c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    common.GroupPrefix + ":" + c.queueID,
		Consumer: c.workerID,
		MinIdle:  minIdleTime,
		Start:    "0",
	}).Result()
	if err != nil {
		return "", fmt.Errorf("failed to claim dangling tasks: %w", err)
	}
	// c.takeTasks(ctx, tasks, taskHAndler)
	return next, nil
}

func (c *queueClient) TakeNewTasks(ctx context.Context, count int, taskHandler func(*common.Task) error) error {
	streamName := common.StreamPrefix + ":" + c.queueID
	res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    common.GroupPrefix + ":" + c.queueID,
		Consumer: c.workerID,
		Streams:  []string{streamName, ">"},
		Block:    time.Second,
		Count:    int64(count),
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get tasks: %w", err)
	}
	return c.takeTasks(ctx, res[0].Messages, taskHandler)
}

func (c *queueClient) TakeTask(ctx context.Context, taskHandler func(*common.Task) error) (err error) {
	next := ""
	for next != "0-0" {
		next, err = c.ClaimDanglingTasks(ctx, time.Hour)
		if err != nil {
			return fmt.Errorf("failed to claim dangling tasks: %w", err)
		}
	}
	err = c.TakePendingTasks(ctx, taskHandler)
	if err != nil {
		return fmt.Errorf("failed to take pending tasks: %w", err)
	}
	err = c.TakeNewTasks(ctx, 1, taskHandler)
	if err != nil {
		return fmt.Errorf("failed to take a new task: %w", err)
	}
	return nil
}
