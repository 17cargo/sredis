package sredis

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type pint int64

func (p pint) Priority() int64 {
	return int64(p)
}

func TestSimpleDelayQueue(t *testing.T) {
	queue := NewSimpleDelayQueue()

	for i := 1; i < 10; i++ {
		for j := 0; j < i; j++ {
			queue.Push((int64(i)), nil)
		}
	}

	for i := 0; i < 10; i++ {
		results := queue.PopMany(int64(i))
		assert.Equal(t, i, len(results))
	}

	// queue is empty
	for i := 0; i < 10; i++ {
		results := queue.PopMany(int64(i))
		assert.Equal(t, 0, len(results))
	}
}

func TestSimpleDelayQueue2(t *testing.T) {
	queue := NewSimpleDelayQueue()

	for i := 1; i < 10; i++ {
		for j := 0; j < i; j++ {
			queue.Push((int64(i)), nil)
		}
	}

	// (1 + 9) * 10 / 2
	results := queue.PopMany(int64(10))
	assert.Equal(t, 45, len(results))

	// queue is empty
	for i := 0; i < 10; i++ {
		results := queue.PopMany(int64(i))
		assert.Equal(t, 0, len(results))
	}
}

func BenchmarkSimpleDelayQueuePush(b *testing.B) {
	queue := NewSimpleDelayQueue()
	for i := 0; i < b.N; i++ {
		queue.Push(rand.Int63(), nil)
	}
}

func BenchmarkSimpleDelayQueuePop(b *testing.B) {
	queue := NewSimpleDelayQueue()
	for i := 0; i < b.N; i++ {
		queue.PopMany(rand.Int63())
	}
}

func BenchmarkSimpleDelayQueuePush2(b *testing.B) {
	queue := NewSimpleDelayQueue()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Push(rand.Int63(), nil)
		}
	})
}

func BenchmarkSimpleDelayQueuePop2(b *testing.B) {
	queue := NewSimpleDelayQueue()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.PopMany(rand.Int63())
		}
	})
}
