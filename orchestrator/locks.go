package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/natexcvi/tpq/common"
)

var (
	errCorruptedLock      = fmt.Errorf("lock has been corrupted")
	errAcquisitionTimeout = fmt.Errorf("timed out trying to acquire lock")
)

type LockHandler interface {
	AcquireLock(ctx context.Context, key string, acquisitionTimeout time.Duration, lockTimeout time.Duration) (lockID string, err error)
	ReleaseLock(ctx context.Context, key string, lockID string) error
}

type lockHandler struct {
	rdb *redis.Client
}

func NewLockHandler(rdb *redis.Client) *lockHandler {
	return &lockHandler{
		rdb: rdb,
	}
}

func (h *lockHandler) AcquireLock(ctx context.Context, key string, acquisitionTimeout time.Duration, lockTimeout time.Duration) (lockID string, err error) {
	tryUntil := time.Now().Add(acquisitionTimeout)
	lockname := common.LockPrefix + ":" + key
	for time.Now().Before(tryUntil) {
		lockID = uuid.NewString()
		if h.rdb.SetNX(ctx, lockname, lockID, lockTimeout).Val() {
			return lockID, nil
		} else if h.rdb.TTL(ctx, lockname).Val() == -1 {
			h.rdb.Expire(ctx, lockname, lockTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", errAcquisitionTimeout
}

func (h *lockHandler) ReleaseLock(ctx context.Context, key string, lockID string) error {
	lockname := common.LockPrefix + ":" + key
	for {
		err := h.rdb.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				if p.Get(ctx, lockname).Val() == lockID {
					p.Del(ctx, lockname)
					return nil
				}
				return errCorruptedLock
			})
			return err
		}, lockname)
		switch err {
		case redis.TxFailedErr:
			continue
		default:
			return fmt.Errorf("failed to release key: %w", err)
		}
	}
}
