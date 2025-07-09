package routing

import (
	"net/http"
	"sync"
)

type batchRequest struct {
	Requests []*http.Request
	Priority int
}

type PriorityQueue struct {
	queues map[int][]*batchRequest
	mutex  sync.Mutex
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{queues: make(map[int][]*batchRequest)}
}

func (pq *PriorityQueue) Push(req *batchRequest) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	pq.queues[req.Priority] = append(pq.queues[req.Priority], req)
}

func (pq *PriorityQueue) Pop() *batchRequest {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	for i := 0; i <= 10; i++ {
		if len(pq.queues[i]) > 0 {
			br := pq.queues[i][0]
			pq.queues[i] = pq.queues[i][1:]
			return br
		}
	}
	return nil
}
