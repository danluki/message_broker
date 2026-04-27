package main

import (
	"fmt"
	"net/http"
	"context"
	"os"
	"time"
	"sync"
	"strconv"
)

type msgData string

type qbroker struct {
	mu sync.Mutex
	queues map[string]*queue
}

type queue struct {
	msgs []msgData
	waiters []*waiter
}

type waiter struct {
	recv chan msgData
	active bool
}

func NewBroker() *qbroker {
	return &qbroker{
		queues: make(map[string]*queue),
	}
}

func (b *qbroker) upsertQueue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = new(queue)
		b.queues[name] = q
	}

	return q
}

func (b *qbroker) put(ctx context.Context, name string, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue := b.upsertQueue(name)

	for len(queue.waiters) > 0 {
		waiter := queue.waiters[0]
		/* Надо для того чтобы память не текла при ленивом удалении*/
		queue.waiters[0] = nil	
		queue.waiters = queue.waiters[1:]

		/*
			Проверка на активность вейтера, нужна, чтобы избежать потерь сообщения
			в случае, если вейтер, ушел в cancel, и больше не ждёт сообщений, а мы
			пишем ему в канал, после чего он не читая это сообщение помечается не активным, и 
			будет удалён.
		*/
		if waiter.active {
			waiter.active = false
			waiter.recv <- msgData(value)
			return
		}

	}
	queue.msgs = append(queue.msgs, msgData(value))
}

func (b *qbroker) get(ctx context.Context, queueName string, timeout time.Duration) (string, bool) {
	b.mu.Lock()

	queue := b.upsertQueue(queueName)

	/* 
		Если пришёл GET, а в очереди есть сообщения, значит вейтеров нет
		этот инвариант проверяется в методе put, там в цикле проверяем
		на то, чтобы в порядке очереде отправить новое сообщение уже
		имеющимся активным вейтерам, поэтому если при получении сообщений
		в очереди есть сообщения, значит вейтеров нет и мы можем сделать fast path.
	*/
	if len(queue.msgs) > 0 {
		msg := queue.msgs[0]
		queue.msgs = queue.msgs[1:]
		b.mu.Unlock()
		return string(msg), true
	}

	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	waiter := &waiter{
		recv: make(chan msgData, 1),
		active: true,
	}

	queue.waiters = append(queue.waiters, waiter)
	b.mu.Unlock()

	select {
		case msg := <-waiter.recv:
			return string(msg), true
		case <-ctx.Done():
			b.cancelWaiter(queueName, waiter)
			return "", false
	}
}

func (b *qbroker) cancelWaiter(queueName string, waiter *waiter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	/*
		Можно удалить вейтеров неактивных вейтеров тут, но будет удаление из середины
		списка а это O(n), при кажддом таймауте, поэтому делаю ленивое удаление, но тут
		минус что при многих GET завершившихся по таймауту без PUT, поиск нового вейтера
		будет занимать O(n)
	*/
	waiter.active = false
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: qbroker <port>")
		os.Exit(1)
	}

	portStr := os.Args[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "usage: qbroker <port>")
		os.Exit(1)
	}

	if port < 0 || port > 65535 {
		fmt.Fprintln(os.Stderr, "usage: qbroker <port>")
		os.Exit(1)
	}

	broker := NewBroker()

	mux := http.NewServeMux()

	mux.HandleFunc("PUT /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queue := r.PathValue("queue")

		value := r.URL.Query().Get("v")
		if value == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		
		broker.put(r.Context(), queue, value)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queue := r.PathValue("queue")

		var timeout time.Duration

		if raw := r.URL.Query().Get("timeout"); raw != "" {
			var err error
			sec, err := strconv.Atoi(raw)
			if err != nil || sec < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			timeout = time.Duration(sec) * time.Second
		}

		msg, ok := broker.get(r.Context(), queue, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_, _ = w.Write([]byte(msg))
	})

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
