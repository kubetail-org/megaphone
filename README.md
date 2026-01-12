# Megaphone

*Megaphone is a tiny, in-memory async broadcast library for Go*

<img width="480" height="355" alt="534581366-a292c530-c598-4560-b4bc-b5a599235211" src="https://github.com/user-attachments/assets/7819f447-51a8-4045-95c4-0dfc15396be4" />

## Introduction

Megaphone is a tiny, in-memory async broadcast library for Go that uses generics to enable simple, type-safe broadcast workflows. Using megaphone you can publish to many subscribers simultaneously in a thread-safe way that fits neatly into common workflows. Although similar patterns have been implemented in existing libraries, we couldn't find a simple one with support for graceful shutdowns, so we created this library to use ourselves (for [https://github.com/kubetail-org/kubetail](Kubetail)). We hope you find it useful too.

## Installation

```bash
go get github.com/kubetail-org/megaphone
```

## Basic Usage

```go
import "github.com/kubetail-org/megaphone"

// Initialize (use type-arg to specify message type)
mp := megaphone.New[string]()

// Publish message (fire-and-forget)
mp.Publish("my-topic", "my-message")

// Subscribe to messages using a callback function (async)
sub, err := mp.Subscribe("my-topic", func(msg string) {
	fmt.Println("from callback: " + msg)
})

// Stop listening for new messages
sub.Unsubscribe()

// Drain subscriber (calls Unsubscribe() then blocks until in-progress are finished)
sub.Drain()

// Drain subscriber with context (e.g. for deadlines)
err = sub.DrainWithContext(ctx)

// Reject new subscribers, silently ignore future publishers
mp.Close()

// Drain all subscribers (calls Close() then blocks until all drains are finished)
mp.Drain()

// Drain all subscribers with context (e.g. for deadlines)
err = mp.DrainWithContext(ctx)
```

## Delivery Guarantees

- **In-memory only**: Messages are not persisted. Publishing to a topic with no subscribers silently drops the message.
- **At-most-once delivery**: Each subscriber receives a message at most once. Concurrent `Unsubscribe` or `Close` calls may cause in-flight messages to be dropped.
- **No ordering guarantees**: Callbacks run in separate goroutines and may complete out of order.
- **Graceful shutdown**: `Unsubscribe` and `Close` prevent new deliveries, but callbacks already in flight will run to completion. Use `Drain` or `DrainWithContext` to wait for in-flight callbacks before shutdown.

## API

### Constructor

#### `New[T]()`

Creates a new Megaphone instance parameterized with a message type.

```go
mp := megaphone.New[string]()
```

### Megaphone

#### `Publish(topic string, message T)`

Publishes a message to all subscribers of a topic. This is a fire-and-forget operation that returns immediately.

```go
mp.Publish("my-topic", "hello world")
```

#### `Subscribe(topic string, callback func(msg T)) (Subscriber, error)`

Creates a subscriber for a topic. Messages are delivered asynchronously via the callback (invoked in a separate goroutine for each message).

```go
sub, err := mp.Subscribe("my-topic", func(msg string) {
    fmt.Println("received: " + msg)
})
```

#### `Close()`

Rejects new subscribers and silently ignores new publishers. In-flight callbacks continue running; use `Drain` if you need to wait for them to finish.

```go
mp.Close()
```

#### `Drain()`

Drains all subscribers, blocking until all pending messages have been processed. Implicitly calls `Close()` when complete.

```go
mp.Drain()
```

#### `DrainWithContext(ctx context.Context) error`

Drains all subscribers with context support for deadlines and cancellation. Returns `nil` on success, or `ctx.Err()` if context is cancelled before drain completes.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := mp.DrainWithContext(ctx); err != nil {
    log.Println("drain timed out, some messages may be in flight")
}
```

### Subscriber

#### `Unsubscribe()`

Removes the subscriber from the topic, stopping it from receiving new messages.

```go
sub.Unsubscribe()
```

#### `Drain()`

Blocks until all pending messages for this subscriber have been processed.

```go
sub.Drain()
```

#### `DrainWithContext(ctx context.Context) error`

Drains the subscriber with context support for deadlines and cancellation. Returns `nil` on success, or `ctx.Err()` if context is cancelled before drain completes.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := sub.DrainWithContext(ctx); err != nil {
    log.Println("drain timed out")
}
```

## Acknowledgements

* The Megaphone API is heavily influenced by [nats.go](https://github.com/nats-io/nats.go)
