package eventx

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type retryConsumerTest struct {
	ctx    context.Context
	cancel func()
	repo   *RepositoryMock[testEvent]

	getFunc  func() (sql.NullInt64, error)
	getCalls int

	setFunc  func() error
	setCalls []uint64

	handlerFunc  func() error
	handlerCalls [][]testEvent

	runner   *Runner[testEvent]
	consumer *RetryConsumer[testEvent]

	consumerWg sync.WaitGroup

	shutdown func()
}

func newRetryConsumerTest(t *testing.T, initEvents []testEvent, options ...RetryConsumerOption) *retryConsumerTest {
	return newRetryConsumerTestWithRunnerSize(t, initEvents, 256, options...)
}

func newRetryConsumerTestWithRunnerSize(
	_ *testing.T, initEvents []testEvent, runnerSize int, options ...RetryConsumerOption,
) *retryConsumerTest {
	repo := &RepositoryMock[testEvent]{}

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return initEvents, nil
	}
	runner := NewRunner[testEvent](repo, setTestEventSeq, WithCoreStoredEventsSize(uint64(runnerSize)))

	ctx, cancel := context.WithCancel(context.Background())

	c := &retryConsumerTest{
		ctx:    ctx,
		cancel: cancel,
		repo:   repo,

		runner:   runner,
		shutdown: func() {},
	}

	consumer := NewRetryConsumer[testEvent](
		runner, repo,
		c.getSequence,
		c.setSequence,
		c.handler,
		options...,
	)
	c.consumer = consumer

	return c
}

func (c *retryConsumerTest) startRunner() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.runner.Run(c.ctx)
	}()

	c.shutdown = func() {
		c.cancel()
		wg.Wait()
	}
}

func (c *retryConsumerTest) getSequence(_ context.Context) (sql.NullInt64, error) {
	c.getCalls++
	return c.getFunc()
}

func (c *retryConsumerTest) setSequence(_ context.Context, seq uint64) error {
	c.setCalls = append(c.setCalls, seq)
	return c.setFunc()
}

func (c *retryConsumerTest) handler(_ context.Context, events []testEvent) error {
	c.handlerCalls = append(c.handlerCalls, events)
	return c.handlerFunc()
}

func (c *retryConsumerTest) runConsumer() {
	c.consumerWg.Add(1)
	go func() {
		defer c.consumerWg.Done()
		c.consumer.RunConsumer(c.ctx)
	}()
}

func TestRetryConsumer_GetLastSequence(t *testing.T) {
	t.Run("get seq return null, not call handler", func(t *testing.T) {
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		})

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{21}, c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		calls := c.repo.GetLastEventsCalls()
		assert.Equal(t, 1, len(calls))
		assert.Equal(t, uint64(1), calls[0].Limit)

		c.shutdown()
	})

	t.Run("get seq return non null, not call handler", func(t *testing.T) {
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		})

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 19,
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{21}, c.setCalls)
		assert.Equal(t, 1, len(c.handlerCalls))
		assert.Equal(t, []testEvent{
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, c.handlerCalls[0])

		c.shutdown()
	})

	t.Run("get seq return error, not yet retried", func(t *testing.T) {
		var loggedErrors []error

		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, errors.New("some error")
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: get sequence: some error", loggedErrors[0].Error())

		c.shutdown()
	})

	t.Run("get seq return error, retried multi times", func(t *testing.T) {
		var loggedErrors []error

		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
		},
			WithConsumerRetryDuration(30*time.Millisecond),
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, errors.New("some error")
		}

		c.runConsumer()

		time.Sleep(75 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 3, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		assert.Equal(t, 3, len(loggedErrors))
		assert.Equal(t, "retry consumer: get sequence: some error", loggedErrors[0].Error())
		assert.Equal(t, "retry consumer: get sequence: some error", loggedErrors[2].Error())

		c.shutdown()
	})

	t.Run("get seq return error after some sleep, not yet retried", func(t *testing.T) {
		var loggedErrors []error

		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			time.Sleep(45 * time.Millisecond)
			return sql.NullInt64{}, errors.New("some error")
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		assert.Equal(t, 0, len(loggedErrors))

		c.shutdown()
	})

	t.Run("get last events returns error, not yet retied", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, nil
		}

		c.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return nil, errors.New("get events")
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: repo.GetLastEvents: get events", loggedErrors[0].Error())

		c.shutdown()
	})

	t.Run("get last events return empty, call set with zero", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, nil,
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, nil
		}
		c.setFunc = func() error {
			return nil
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, 0, len(loggedErrors))
		assert.Equal(t, []uint64{0}, c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		c.shutdown()
	})

	t.Run("get last events return empty, call set return error", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, nil,
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, nil
		}
		c.setFunc = func() error {
			return errors.New("set err")
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: set sequence: set err", loggedErrors[0].Error())

		assert.Equal(t, []uint64{0}, c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		c.shutdown()
	})

	t.Run("get last events return non empty, call set return error", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 31, seq: 19},
		},
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{}, nil
		}
		c.setFunc = func() error {
			return errors.New("set err")
		}

		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: set sequence: set err", loggedErrors[0].Error())

		assert.Equal(t, []uint64{19}, c.setCalls)
		assert.Equal(t, 0, len(c.handlerCalls))

		c.shutdown()
	})
}

func TestRetryConsumer_Handler(t *testing.T) {
	t.Run("call handler with error, not yet retried", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 18,
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return errors.New("handle error")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)
		assert.Equal(t, 1, len(c.handlerCalls))
		assert.Equal(t, []testEvent{
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, c.handlerCalls[0])

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: handler: handle error", loggedErrors[0].Error())

		c.shutdown()
	})

	t.Run("call handler with error, retried multi times", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		},
			WithConsumerRetryDuration(30*time.Millisecond),
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 18,
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return errors.New("handle error")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(75 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)

		assert.Equal(t, [][]testEvent{
			{
				{id: 28, seq: 19},
				{id: 33, seq: 20},
				{id: 32, seq: 21},
			},
			{
				{id: 28, seq: 19},
				{id: 33, seq: 20},
				{id: 32, seq: 21},
			},
			{
				{id: 28, seq: 19},
				{id: 33, seq: 20},
				{id: 32, seq: 21},
			},
		}, c.handlerCalls)

		assert.Equal(t, 3, len(loggedErrors))
		assert.Equal(t, "retry consumer: handler: handle error", loggedErrors[0].Error())
		assert.Equal(t, "retry consumer: handler: handle error", loggedErrors[2].Error())

		c.shutdown()
	})

	t.Run("call handler with error, after some sleep", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 18,
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			time.Sleep(45 * time.Millisecond)
			return errors.New("handle error")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64(nil), c.setCalls)
		assert.Equal(t, 1, len(c.handlerCalls))
		assert.Equal(t, []testEvent{
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, c.handlerCalls[0])

		assert.Equal(t, 0, len(loggedErrors))

		c.shutdown()
	})

	t.Run("set sequence error, multiple times", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		},
			WithConsumerRetryDuration(30*time.Millisecond),
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 18,
			}, nil
		}
		c.setFunc = func() error {
			return errors.New("set err")
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(75 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{21, 21, 21}, c.setCalls)

		assert.Equal(t, 3, len(loggedErrors))
		assert.Equal(t, "retry consumer: set sequence: set err", loggedErrors[0].Error())
		assert.Equal(t, "retry consumer: set sequence: set err", loggedErrors[2].Error())

		c.shutdown()
	})

	t.Run("set sequence error, after some sleep", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		},
			WithConsumerRetryDuration(30*time.Millisecond),
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 18,
			}, nil
		}
		c.setFunc = func() error {
			time.Sleep(45 * time.Millisecond)
			return errors.New("set err")
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{21}, c.setCalls)

		assert.Equal(t, 0, len(loggedErrors))

		c.shutdown()
	})

	t.Run("subscriber fetch error", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
		}, WithRetryConsumerErrorLogger(func(err error) {
			loggedErrors = append(loggedErrors, err)
		}))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 16,
			}, nil
		}
		c.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
			return nil, errors.New("db err")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		assert.Equal(t, 1, len(loggedErrors))
		assert.Equal(t, "retry consumer: subscriber fetch: db err", loggedErrors[0].Error())

		c.shutdown()
	})

	t.Run("subscriber fetch error multiple times", func(t *testing.T) {
		var loggedErrors []error
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
		},
			WithConsumerRetryDuration(30*time.Millisecond),
			WithRetryConsumerErrorLogger(func(err error) {
				loggedErrors = append(loggedErrors, err)
			}),
		)

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 16,
			}, nil
		}
		c.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
			return nil, errors.New("db err")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(75 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		assert.Equal(t, 3, len(loggedErrors))
		assert.Equal(t, "retry consumer: subscriber fetch: db err", loggedErrors[0].Error())
		assert.Equal(t, "retry consumer: subscriber fetch: db err", loggedErrors[2].Error())

		c.shutdown()
	})

	t.Run("subscriber fetch error, with default logger", func(t *testing.T) {
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
		})

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 16,
			}, nil
		}
		c.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
			return nil, errors.New("db err")
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)

		c.shutdown()
	})
}

func TestRetryConsumer_Normal(t *testing.T) {
	t.Run("have existing events, get return zero", func(t *testing.T) {
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 4},
			{id: 28, seq: 5},
		})

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 0,
			}, nil
		}
		c.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 11, seq: 1},
				{id: 12, seq: 2},
				{id: 13, seq: 3},
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{3, 5}, c.setCalls)
		assert.Equal(t, [][]testEvent{
			{
				{id: 11, seq: 1},
				{id: 12, seq: 2},
				{id: 13, seq: 3},
			},
			{
				{id: 30, seq: 4},
				{id: 28, seq: 5},
			},
		}, c.handlerCalls)

		getCalls := c.repo.GetEventsFromCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, uint64(1), getCalls[0].From)
		assert.Equal(t, uint64(16), getCalls[0].Limit)

		c.shutdown()
	})

	t.Run("have existing events, with fetch limit", func(t *testing.T) {
		c := newRetryConsumerTest(t, []testEvent{
			{id: 30, seq: 4},
			{id: 28, seq: 5},
		}, WithRetryConsumerFetchLimit(64))

		c.getFunc = func() (sql.NullInt64, error) {
			return sql.NullInt64{
				Valid: true,
				Int64: 2,
			}, nil
		}
		c.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 13, seq: 3},
			}, nil
		}
		c.setFunc = func() error {
			return nil
		}
		c.handlerFunc = func() error {
			return nil
		}

		c.startRunner()
		c.runConsumer()

		time.Sleep(30 * time.Millisecond)
		c.cancel()
		c.consumerWg.Wait()

		assert.Equal(t, 1, c.getCalls)
		assert.Equal(t, []uint64{3, 5}, c.setCalls)
		assert.Equal(t, [][]testEvent{
			{
				{id: 13, seq: 3},
			},
			{
				{id: 30, seq: 4},
				{id: 28, seq: 5},
			},
		}, c.handlerCalls)

		getCalls := c.repo.GetEventsFromCalls()
		assert.Equal(t, 1, len(getCalls))
		assert.Equal(t, uint64(3), getCalls[0].From)
		assert.Equal(t, uint64(64), getCalls[0].Limit)

		c.shutdown()
	})
}
