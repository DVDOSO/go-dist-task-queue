package taskq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// recordingBroker captures what a Client hands to a broker. It implements only
// Enqueue because that is all Client touches; the rest panics loudly rather
// than silently returning zero values if that ever stops being true.
type recordingBroker struct {
	got *Job
	err error
}

func (r *recordingBroker) Enqueue(_ context.Context, j *Job) error {
	if r.err != nil {
		return r.err
	}
	r.got = j
	return nil
}

func (r *recordingBroker) Dequeue(context.Context, ClaimOpts) (*Job, error) {
	panic("unexpected Dequeue")
}
func (r *recordingBroker) Ack(context.Context, string, string) error { panic("unexpected Ack") }
func (r *recordingBroker) Nack(context.Context, string, string, time.Time, string) error {
	panic("unexpected Nack")
}
func (r *recordingBroker) Kill(context.Context, string, string, string) error {
	panic("unexpected Kill")
}
func (r *recordingBroker) Extend(context.Context, string, []string, time.Duration) ([]string, error) {
	panic("unexpected Extend")
}
func (r *recordingBroker) Close() error { return nil }

var _ Broker = (*recordingBroker)(nil)

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	rb := &recordingBroker{}
	c := NewClient(rb)

	job, err := c.EnqueueTask(context.Background(), "email:send", map[string]int{"n": 1})
	if err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	if job.Queue != DefaultQueue {
		t.Errorf("Queue = %q, want %q", job.Queue, DefaultQueue)
	}
	if job.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", job.MaxAttempts, DefaultMaxAttempts)
	}
	if job.Type != "email:send" {
		t.Errorf("Type = %q", job.Type)
	}
	if string(job.Payload) != `{"n":1}` {
		t.Errorf("Payload = %s", job.Payload)
	}
	if rb.got != job {
		t.Error("the job handed to the broker should be the one returned")
	}
}

func TestClientOptions(t *testing.T) {
	t.Parallel()

	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		opts  []EnqueueOption
		check func(t *testing.T, j *Job)
	}{
		{
			name: "queue",
			opts: []EnqueueOption{Queue("critical")},
			check: func(t *testing.T, j *Job) {
				if j.Queue != "critical" {
					t.Errorf("Queue = %q", j.Queue)
				}
			},
		},
		{
			name: "max attempts",
			opts: []EnqueueOption{MaxAttempts(2)},
			check: func(t *testing.T, j *Job) {
				if j.MaxAttempts != 2 {
					t.Errorf("MaxAttempts = %d", j.MaxAttempts)
				}
			},
		},
		{
			name: "at",
			opts: []EnqueueOption{At(at)},
			check: func(t *testing.T, j *Job) {
				if !j.RunAt.Equal(at) {
					t.Errorf("RunAt = %v, want %v", j.RunAt, at)
				}
			},
		},
		{
			name: "delay",
			opts: []EnqueueOption{Delay(time.Hour)},
			check: func(t *testing.T, j *Job) {
				if time.Until(j.RunAt) < 59*time.Minute {
					t.Errorf("RunAt = %v, want about an hour out", j.RunAt)
				}
			},
		},
		{
			name: "unique",
			opts: []EnqueueOption{Unique("k", time.Minute)},
			check: func(t *testing.T, j *Job) {
				if j.UniqueKey != "k" || j.UniqueTTL != time.Minute {
					t.Errorf("unique = %q/%v", j.UniqueKey, j.UniqueTTL)
				}
			},
		},
		{
			name: "later options win",
			opts: []EnqueueOption{Queue("first"), Queue("second")},
			check: func(t *testing.T, j *Job) {
				if j.Queue != "second" {
					t.Errorf("Queue = %q, want second", j.Queue)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(&recordingBroker{})
			job, err := c.EnqueueTask(context.Background(), "t", nil, tt.opts...)
			if err != nil {
				t.Fatalf("EnqueueTask: %v", err)
			}
			tt.check(t, job)
		})
	}
}

func TestClientConfiguredDefaults(t *testing.T) {
	t.Parallel()

	c := NewClient(&recordingBroker{},
		WithDefaultQueue("bulk"),
		WithDefaultMaxAttempts(4))

	job, err := c.EnqueueTask(context.Background(), "t", nil)
	if err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}
	if job.Queue != "bulk" || job.MaxAttempts != 4 {
		t.Errorf("job = queue %q attempts %d, want bulk/4", job.Queue, job.MaxAttempts)
	}
}

func TestClientRejectsNilTask(t *testing.T) {
	t.Parallel()

	c := NewClient(&recordingBroker{})
	if _, err := c.Enqueue(context.Background(), nil); !errors.Is(err, ErrInvalidTask) {
		t.Errorf("Enqueue(nil) = %v, want ErrInvalidTask", err)
	}
}

func TestClientPropagatesBrokerError(t *testing.T) {
	t.Parallel()

	want := errors.New("redis is down")
	c := NewClient(&recordingBroker{err: want})

	if _, err := c.EnqueueTask(context.Background(), "t", nil); !errors.Is(err, want) {
		t.Errorf("EnqueueTask = %v, want the broker's error", err)
	}
}
