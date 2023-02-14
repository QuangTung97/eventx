package helpers

import (
	"context"
	"fmt"
	"github.com/QuangTung97/eventx"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type propertyTest struct {
	repo   *repoTest
	runner *eventx.Runner[testEvent]
}

func newPropertyTest() *propertyTest {
	p := &propertyTest{}
	p.repo = newRepoTest()

	p.runner = eventx.NewRunner[testEvent](
		p.repo,
		func(event *testEvent, seq uint64) {
			event.seq = seq
		},
	)

	return p
}

//revive:disable-next-line:cognitive-complexity
func TestEventx_Property_Based(*testing.T) {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	fmt.Println("SEED:", seed)

	p := newPropertyTest()

	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer wg.Done()

		p.runner.Run(ctx)
	}()

	var nextEventData atomic.Uint64
	for th := 0; th < 5; th++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 1000; i++ {
				batchSize := rand.Intn(10) + 1
				var events []testEvent
				for k := 0; k < batchSize; k++ {
					nextData := nextEventData.Add(1)
					events = append(events, testEvent{
						data: int(nextData),
					})
				}

				p.repo.insertEvents(events)
				p.runner.Signal()
			}
		}()
	}

	const numSubscriber = 4
	subEvents := make([][]testEvent, numSubscriber)
	for th := 0; th < numSubscriber; th++ {
		threadIndex := th

		wg.Add(1)

		sub := p.runner.NewSubscriber(1, 3)

		go func() {
			defer wg.Done()

			for {
				events, err := sub.Fetch(ctx)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					panic(err)
				}

				subEvents[threadIndex] = append(subEvents[threadIndex], events...)
			}
		}()
	}

	time.Sleep(1 * time.Second)

	cancel()

	wg.Wait()

	for i := 0; i < numSubscriber; i++ {
		fmt.Println("EVENT LEN:", len(subEvents[i]))

		prev := uint64(0)
		prevID := int64(0)

		outOfOrders := 0

		for _, e := range subEvents[i] {
			if e.seq != prev+1 {
				panic("Invalid sequence value")
			}

			if e.id < prevID {
				outOfOrders++
			}

			prev = e.seq
			prevID = e.id
		}

		fmt.Println("Out of Order Count:", outOfOrders)
	}
}
