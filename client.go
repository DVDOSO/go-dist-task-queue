package taskq

import (
	"context"
	"fmt"
	"time"
)

// DefaultQueue is used by jobs enqueued without an explicit queue.
const DefaultQueue = "default"

// Client is the producer API: it turns a [Task] into a [Job] and hands it to a
// broker.
//
// It is a thin layer on purpose. Its only real job is defaulting, so that the
// broker can stay strict and reject an under-specified job rather than guessing
// what the caller meant.
type Client struct {
	broker Broker

	defaultQueue       string
	defaultMaxAttempts int
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithDefaultQueue changes the queue used when an enqueue does not name one.
func WithDefaultQueue(q string) ClientOption {
	return func(c *Client) { c.defaultQueue = q }
}

// WithDefaultMaxAttempts changes the attempt budget given to jobs that do not
// specify one.
func WithDefaultMaxAttempts(n int) ClientOption {
	return func(c *Client) { c.defaultMaxAttempts = n }
}

// NewClient returns a producer bound to a broker.
func NewClient(b Broker, opts ...ClientOption) *Client {
	c := &Client{
		broker:             b,
		defaultQueue:       DefaultQueue,
		defaultMaxAttempts: DefaultMaxAttempts,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// EnqueueOption customises a single enqueue.
type EnqueueOption func(*Job)

// Queue routes the job to a named queue.
func Queue(name string) EnqueueOption {
	return func(j *Job) { j.Queue = name }
}

// MaxAttempts sets how many times the job may be claimed before it is
// dead-lettered.
func MaxAttempts(n int) EnqueueOption {
	return func(j *Job) { j.MaxAttempts = n }
}

// Delay makes the job eligible to run after d has elapsed.
func Delay(d time.Duration) EnqueueOption {
	return func(j *Job) { j.RunAt = time.Now().Add(d) }
}

// At makes the job eligible to run at a specific time.
func At(t time.Time) EnqueueOption {
	return func(j *Job) { j.RunAt = t }
}

// Unique collapses duplicate enqueues sharing key within ttl.
//
// The window is a TTL rather than "until this job finishes" so that a producer
// crashing between enqueue and completion cannot wedge the key permanently.
func Unique(key string, ttl time.Duration) EnqueueOption {
	return func(j *Job) {
		j.UniqueKey = key
		j.UniqueTTL = ttl
	}
}

// Enqueue submits a task and returns the job that was created.
//
// A nil error is the point at which the at-least-once guarantee attaches: from
// here the job will be delivered to a handler at least once. An error may be
// ambiguous -- a timed-out round trip could mean the job landed or did not --
// so producers that care should pass [Unique] and treat [ErrDuplicate] as
// success.
func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...EnqueueOption) (*Job, error) {
	if task == nil {
		return nil, fmt.Errorf("%w: nil task", ErrInvalidTask)
	}

	j := &Job{
		Type:        task.Type,
		Payload:     task.Payload,
		Queue:       c.defaultQueue,
		MaxAttempts: c.defaultMaxAttempts,
	}
	for _, opt := range opts {
		opt(j)
	}

	if err := c.broker.Enqueue(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

// EnqueueTask is shorthand for building a task and enqueuing it in one call.
func (c *Client) EnqueueTask(ctx context.Context, taskType string, payload any, opts ...EnqueueOption) (*Job, error) {
	task, err := NewTask(taskType, payload)
	if err != nil {
		return nil, err
	}
	return c.Enqueue(ctx, task, opts...)
}
