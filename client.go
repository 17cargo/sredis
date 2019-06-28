package sredis

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	Idle int32 = iota
	Running
)

const (
	HasNoData int32 = iota
	HasData
)

var RedisTimeOutErr = errors.New("redis time out error")

type Request struct {
	cmd    string
	args   []interface{}
	future *Future
}

type Future struct {
	r       interface{}
	err     error
	wait    chan struct{}
	mu      *sync.RWMutex
	timeout bool
}

func (f *Future) SetTimeout() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.timeout = true
}

func (f *Future) IsTimeout() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.timeout
}

type FutureChan struct {
	futures []*Request
	conn    redigo.Conn
}

type Client struct {
	batchSize              int
	requestQueue           *RingQueue
	requestSchedulerStatus int32
	requestCnt             int32

	futureQueue           *RingQueue
	futureSchedulerStatus int32
	futureCnt             int32

	mgr   *RoundRobinAsyncClient
	index int
	conn  redigo.Conn
	mu    *sync.RWMutex
}

func NewClient(mgr *RoundRobinAsyncClient, index int, conn redigo.Conn) *Client {
	return &Client{
		mgr:          mgr,
		index:        index,
		batchSize:    200,
		requestQueue: NewRingQueue(10000),
		futureQueue:  NewRingQueue(10000),
		conn:         conn,
		mu:           &sync.RWMutex{},
	}
}

func (cli *Client) ResetConn(conn redigo.Conn) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	cli.conn = conn
}

func (cli *Client) getConn() redigo.Conn {
	cli.mu.RLock()
	defer cli.mu.RUnlock()
	return cli.conn
}

func (cli *Client) AsyncDo(cmd string, args ...interface{}) (interface{}, error) {
	future := &Future{
		wait: make(chan struct{}),
		mu:   &sync.RWMutex{},
	}
	cli.requestQueue.Push(&Request{cmd, args, future})
	cli.requestSchedule()

	timer := time.NewTimer(2 * time.Second)
	select {
	case <-future.wait:
		timer.Stop()
		return future.r, future.err
	case <-timer.C:
		future.SetTimeout()
		return nil, RedisTimeOutErr
	}
}

func (cli *Client) postFuture(data interface{}) {
	cli.futureQueue.Push(data)
	cli.futureSchedule()
}

func (cli *Client) requestSchedule() {
	atomic.StoreInt32(&cli.requestCnt, HasData)
	if atomic.CompareAndSwapInt32(&cli.requestSchedulerStatus, Idle, Running) {
		go cli.processRequest()
	}
}

func (cli *Client) futureSchedule() {
	atomic.StoreInt32(&cli.futureCnt, HasData)
	if atomic.CompareAndSwapInt32(&cli.futureSchedulerStatus, Idle, Running) {
		go cli.processFuture()
	}
}

func (cli *Client) processRequest() {
	atomic.StoreInt32(&cli.requestCnt, HasNoData)
process:
	cli.requestRun()
	atomic.StoreInt32(&cli.requestSchedulerStatus, Idle)
	if atomic.SwapInt32(&cli.requestCnt, HasNoData) == HasData {
		if atomic.CompareAndSwapInt32(&cli.requestSchedulerStatus, Idle, Running) {
			goto process
		}
	}
}

func (cli *Client) processFuture() {
	atomic.StoreInt32(&cli.futureCnt, HasNoData)
process:
	cli.futureRun()
	atomic.StoreInt32(&cli.futureSchedulerStatus, Idle)
	if atomic.SwapInt32(&cli.futureCnt, HasNoData) == HasData {
		if atomic.CompareAndSwapInt32(&cli.futureSchedulerStatus, Idle, Running) {
			goto process
		}
	}
}

func (cli *Client) requestRun() {
	var err error
	if v, ok := cli.requestQueue.PopMany(int64(cli.batchSize)); ok {
		var futures []*Request
		for _, vv := range v {
			req := vv.(*Request)
			if req.future.IsTimeout() {
				continue
			}
			err = cli.getConn().Send(req.cmd, req.args...)
			if err != nil {
				fmt.Println("Send with err:", err)
				cli.mgr.ResetClientConn(cli.index, cli)
				break
			}
			futures = append(futures, req)
		}
		if err != nil {
			for _, vv := range v {
				req := vv.(*Request)
				req.future.err = err
				close(req.future.wait)
			}
			return
		}
		if len(futures) == 0 {
			return
		}
		err = cli.getConn().Flush()
		if err != nil {
			fmt.Println("Flush with err:", err)
			cli.mgr.ResetClientConn(cli.index, cli)
			for _, req := range futures {
				req.future.err = err
				close(req.future.wait)
			}
			return
		}

		cli.postFuture(&FutureChan{futures, cli.getConn()})
	}
}

func (cli *Client) futureRun() {
	var err error
	if v, ok := cli.futureQueue.PopMany(int64(cli.batchSize)); ok {
		for _, vv := range v {
			futureCh := vv.(*FutureChan)
			for _, req := range futureCh.futures {
				if err != nil {
					req.future.err = err
					close(req.future.wait)
					continue
				}
				var resp interface{}
				resp, err = futureCh.conn.Receive()
				req.future.r = resp
				req.future.err = err
				close(req.future.wait)
			}
		}
	}
}
