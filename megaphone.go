package megaphone

import (
	"context"
	"sync"
)

// Megaphone is the interface for a type-safe, in-memory async broadcast hub.
type Megaphone[T any] interface {
	Publish(topic string, message T)
	Subscribe(topic string, callback func(msg T)) (Subscriber[T], error)
	Close()
	Drain()
	DrainWithContext(ctx context.Context) error
}

// megaphone is the concrete implementation of Megaphone.
type megaphone[T any] struct {
	mu          sync.RWMutex
	subscribers map[string]map[*subscriber[T]]struct{}
	closed      bool
}

// New creates a new Megaphone instance parameterized with a message type.
func New[T any]() Megaphone[T] {
	return &megaphone[T]{
		subscribers: make(map[string]map[*subscriber[T]]struct{}),
	}
}

// Publish sends a message to all subscribers of a topic.
// This is a fire-and-forget operation that returns immediately.
func (mp *megaphone[T]) Publish(topic string, message T) {
	mp.mu.RLock()
	if mp.closed {
		mp.mu.RUnlock()
		return
	}

	subs := make([]*subscriber[T], 0, len(mp.subscribers[topic]))
	for sub := range mp.subscribers[topic] {
		subs = append(subs, sub)
	}
	mp.mu.RUnlock()

	for _, sub := range subs {
		sub.deliver(message)
	}
}

// Subscribe creates a subscriber for a topic. Messages are delivered
// asynchronously via the callback (invoked in a separate goroutine for each message).
func (mp *megaphone[T]) Subscribe(topic string, callback func(msg T)) (Subscriber[T], error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.closed {
		return nil, ErrClosed
	}

	sub := &subscriber[T]{
		callback: callback,
		megaphone:   mp,
		topic:    topic,
	}

	if mp.subscribers[topic] == nil {
		mp.subscribers[topic] = make(map[*subscriber[T]]struct{})
	}
	mp.subscribers[topic][sub] = struct{}{}

	return sub, nil
}

// Close cancels in-flight messages, rejects new subscribers,
// and silently ignores new publishers.
func (mp *megaphone[T]) Close() {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.closed {
		return
	}
	mp.closed = true

	for _, sub := range mp.getSubscribers() {
		sub.markUnsubscribed()
	}
}

// Drain drains all subscribers, blocking until all pending messages
// have been processed. Implicitly calls Close() when complete.
func (mp *megaphone[T]) Drain() {
	mp.Close()

	var wg sync.WaitGroup
	for _, sub := range mp.getSubscribers() {
		wg.Add(1)
		go func(s *subscriber[T]) {
			defer wg.Done()
			s.Drain()
		}(sub)
	}
	wg.Wait()
}

// DrainWithContext drains all subscribers with context support
// for deadlines and cancellation. Returns ctx.Err() if context
// is cancelled before drain completes.
func (mp *megaphone[T]) DrainWithContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		mp.Drain()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Returns flat list of subscribers
func (mp *megaphone[T]) getSubscribers() []*subscriber[T] {
	subs := make([]*subscriber[T], 0)
	for _, topicSubs := range mp.subscribers {
		for sub := range topicSubs {
			subs = append(subs, sub)
		}
	}
	return subs
}

// Removes a specific subscriber from the topics map
func (mp *megaphone[T]) removeSubscriber(sub *subscriber[T]) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	delete(mp.subscribers[sub.topic], sub)
	if len(mp.subscribers[sub.topic]) == 0 {
		delete(mp.subscribers, sub.topic)
	}
}

// Subscriber is the interface for a subscription to a topic.
type Subscriber[T any] interface {
	Unsubscribe()
	Drain()
	DrainWithContext(ctx context.Context) error
}

// subscriber is the concrete implementation of Subscriber.
type subscriber[T any] struct {
	callback     func(T)
	megaphone    *megaphone[T]
	topic        string
	mu           sync.Mutex
	unsubscribed bool
	wg           sync.WaitGroup
}

// Unsubscribe removes the subscriber from the topic,
// stopping it from receiving new messages.
func (s *subscriber[T]) Unsubscribe() {
	if !s.markUnsubscribed() {
		return
	}

	s.megaphone.removeSubscriber(s)
}

// Drain blocks until all pending messages for this subscriber
// have been processed.
func (s *subscriber[T]) Drain() {
	s.wg.Wait()
}

// DrainWithContext drains the subscriber with context support
// for deadlines and cancellation. Returns ctx.Err() if context
// is cancelled before drain completes.
func (s *subscriber[T]) DrainWithContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.Drain()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscriber[T]) deliver(message T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsubscribed {
		return
	}

	cb := s.callback
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		cb(message)
	}()
}

func (s *subscriber[T]) markUnsubscribed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsubscribed {
		return false
	}

	s.unsubscribed = true
	return true
}
