package sredis

import (
	"container/heap"
	"sync"

	redigo "github.com/garyburd/redigo/redis"
)

type CloseConnectionEvent struct {
	deadline int64
	conn     redigo.Conn
}

type CloseConnectionEventQueue []CloseConnectionEvent

func (q *CloseConnectionEventQueue) Len() int {
	return len(*q)
}

func (q *CloseConnectionEventQueue) Less(i, j int) bool {
	return (*q)[i].deadline < (*q)[j].deadline
}

func (q *CloseConnectionEventQueue) Swap(i, j int) {
	if i < 0 || j < 0 {
		return
	}
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *CloseConnectionEventQueue) Pop() interface{} {
	old := *q
	n := q.Len()
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

func (q *CloseConnectionEventQueue) Push(x interface{}) {
	*q = append(*q, x.(CloseConnectionEvent))
}

type SimpleDelayQueue struct {
	queue CloseConnectionEventQueue
	mutex *sync.Mutex
}

func NewSimpleDelayQueue() *SimpleDelayQueue {
	return &SimpleDelayQueue{
		mutex: &sync.Mutex{},
	}
}

func (q *SimpleDelayQueue) Push(deadline int64, conn redigo.Conn) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	heap.Push(&q.queue, CloseConnectionEvent{
		deadline: deadline,
		conn:     conn,
	})
}

func (q *SimpleDelayQueue) PopMany(deadline int64) []redigo.Conn {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	var results []redigo.Conn
	for {
		res := heap.Pop(&q.queue)
		if res == nil {
			break
		}
		event, ok := res.(CloseConnectionEvent)
		if !ok {
			continue
		}
		if event.deadline <= deadline {
			results = append(results, event.conn)
		} else {
			heap.Push(&q.queue, event)
			break
		}
	}
	return results
}
