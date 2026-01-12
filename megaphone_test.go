package megaphone

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbackSubscriber(t *testing.T) {
	mp := New[string]()

	var received string
	var wg sync.WaitGroup

	wg.Add(1)
	sub, err := mp.Subscribe("my-topic", func(msg string) {
		received = msg
		wg.Done()
	})
	require.NoError(t, err)

	mp.Publish("my-topic", "hello")
	wg.Wait()

	assert.Equal(t, "hello", received)

	sub.Unsubscribe()
}

func TestMultipleSubscribers(t *testing.T) {
	mp := New[string]()

	var mu sync.Mutex
	var results []string
	var wg sync.WaitGroup
	wg.Add(2)

	mp.Subscribe("my-topic", func(msg string) {
		mu.Lock()
		results = append(results, "sub1:"+msg)
		mu.Unlock()
		wg.Done()
	})

	mp.Subscribe("my-topic", func(msg string) {
		mu.Lock()
		results = append(results, "sub2:"+msg)
		mu.Unlock()
		wg.Done()
	})

	mp.Publish("my-topic", "hello")
	wg.Wait()

	assert.Len(t, results, 2)
}

func TestDrain(t *testing.T) {
	mp := New[string]()

	var count int
	var mu sync.Mutex

	mp.Subscribe("my-topic", func(msg string) {
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		count++
		mu.Unlock()
	})

	for i := 0; i < 5; i++ {
		mp.Publish("my-topic", "msg")
	}

	mp.Drain()

	mu.Lock()
	assert.Equal(t, 5, count)
	mu.Unlock()
}

func TestDrainWithContext(t *testing.T) {
	mp := New[string]()

	mp.Subscribe("my-topic", func(msg string) {
		time.Sleep(10 * time.Millisecond)
	})

	mp.Publish("my-topic", "msg")

	err := mp.DrainWithContext(context.Background())
	assert.NoError(t, err)
}

func TestDrainWithContextTimeout(t *testing.T) {
	mp := New[string]()

	mp.Subscribe("my-topic", func(msg string) {
		time.Sleep(100 * time.Millisecond)
	})

	mp.Publish("my-topic", "msg")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := mp.DrainWithContext(ctx)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 50*time.Millisecond)
}

func TestSubscriberDrain(t *testing.T) {
	mp := New[string]()

	var done bool
	sub, _ := mp.Subscribe("my-topic", func(msg string) {
		time.Sleep(10 * time.Millisecond)
		done = true
	})

	mp.Publish("my-topic", "msg")
	sub.Drain()

	assert.True(t, done)
}

func TestClose(t *testing.T) {
	mp := New[string]()

	mp.Subscribe("my-topic", func(msg string) {})
	mp.Close()

	// New subscriptions should fail
	_, err := mp.Subscribe("my-topic", func(msg string) {})
	assert.ErrorIs(t, err, ErrClosed)
}

func TestUnsubscribe(t *testing.T) {
	mp := New[string]()

	var count int
	var mu sync.Mutex
	sub, _ := mp.Subscribe("my-topic", func(msg string) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	mp.Publish("my-topic", "msg1")
	sub.Drain()
	sub.Unsubscribe()

	mp.Publish("my-topic", "msg2")
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, count)
	mu.Unlock()
}

func TestMultipleTopics(t *testing.T) {
	mp := New[string]()

	var results []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	mp.Subscribe("topic-a", func(msg string) {
		mu.Lock()
		results = append(results, "a:"+msg)
		mu.Unlock()
		wg.Done()
	})

	mp.Subscribe("topic-b", func(msg string) {
		mu.Lock()
		results = append(results, "b:"+msg)
		mu.Unlock()
		wg.Done()
	})

	mp.Publish("topic-a", "hello")
	mp.Publish("topic-b", "world")
	wg.Wait()

	assert.Len(t, results, 2)
}

func TestPublishToNonexistentTopic(t *testing.T) {
	mp := New[string]()

	// Should not panic
	mp.Publish("nonexistent", "msg")
}

func TestPublishAfterClose(t *testing.T) {
	mp := New[string]()

	var called bool
	mp.Subscribe("my-topic", func(msg string) {
		called = true
	})

	mp.Close()
	mp.Publish("my-topic", "msg")

	time.Sleep(10 * time.Millisecond)

	assert.False(t, called)
}
