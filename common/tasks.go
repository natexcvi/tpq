package common

import (
	"encoding/json"
	"fmt"
	"time"
)

// type Task interface {
// 	ID() string
// 	Priority() float64
// 	Data() interface{}
// }

type TaskQueue interface {
}

type Task struct {
	ID           string
	Scheduled    time.Time
	Priority     float64
	ExecuteAfter time.Time
	Data         interface{}
}

func NewTask(id string, scheduled time.Time, priority float64, executeAfter time.Time, data interface{}) *Task {
	return &Task{
		ID:           id,
		Scheduled:    scheduled,
		Priority:     priority,
		ExecuteAfter: executeAfter,
		Data:         data,
	}
}

func NewEmptyTask() *Task {
	return &Task{}
}

func (t *Task) EncodeJSON() (string, error) {
	encoded, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("failed to encode task: %w", err)
	}
	return string(encoded), nil
}

func ParseTask(taskJson string) (*Task, error) {
	task := NewEmptyTask()
	err := json.Unmarshal([]byte(taskJson), task)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task: %w", err)
	}
	return task, nil
}