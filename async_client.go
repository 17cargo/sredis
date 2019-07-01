package sredis

import (
	"errors"
	"sync/atomic"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

type RoundRobinAsyncClient struct {
	Dial       func() (redigo.Conn, error)
	connectCnt int
	clients    []*Client
	index      uint64

	closeDelayTime      int64
	closeDelayTaskQueue *SimpleDelayQueue
}

type RoundRobinAsyncClientOption func(cli *RoundRobinAsyncClient)

func NewRoundRobinAsyncClient(newFn func() (redigo.Conn, error), opts ...RoundRobinAsyncClientOption) *RoundRobinAsyncClient {
	ret := &RoundRobinAsyncClient{
		Dial:                newFn,
		connectCnt:          10,
		clients:             make([]*Client, 0),
		closeDelayTime:      5,
		closeDelayTaskQueue: NewSimpleDelayQueue(),
	}
	for i := range opts {
		opts[i](ret)
	}
	ret.init()
	go ret.closeLoop()
	return ret
}

func SetRoundRobinAsyncClientConnectionCount(count int) RoundRobinAsyncClientOption {
	return func(cli *RoundRobinAsyncClient) {
		cli.connectCnt = count
	}
}

func SetRoundRobinAsyncClientCloseDelayTime(t int64) RoundRobinAsyncClientOption {
	return func(cli *RoundRobinAsyncClient) {
		cli.closeDelayTime = t
	}
}

func (cli *RoundRobinAsyncClient) init() {
	for i := 0; i < cli.connectCnt; i++ {
		c, err := cli.Dial()
		if err == nil {
			client := NewClient(cli, i, c)
			cli.clients = append(cli.clients, client)
		}
	}
}

func (cli *RoundRobinAsyncClient) get() int {
	l := uint64(len(cli.clients))
	if l <= 0 {
		return -1
	}
	return int(atomic.AddUint64(&cli.index, 1) % l)
}

func (cli *RoundRobinAsyncClient) ResetClientConn(index int, client *Client) {
	for {
		c, err := cli.Dial()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		oldConn := cli.clients[index].ResetConn(c)
		cli.closeDelayTaskQueue.Push(time.Now().Unix()+cli.closeDelayTime, oldConn)
		break
	}
}

//异步
func (cli *RoundRobinAsyncClient) AsyncDo(cmd string, args ...interface{}) (interface{}, error) {
	if len(cli.clients) == 0 {
		return nil, errors.New("no clients")
	}
	index := cli.get()
	if index < 0 {
		return nil, errors.New("no clients")
	}
	return cli.clients[index].AsyncDo(cmd, args...)
}

func (cli RoundRobinAsyncClient) closeLoop() {
	for {
		connections := cli.closeDelayTaskQueue.PopMany(time.Now().Unix())
		for i := range connections {
			if connections[i] != nil {
				connections[i].Close()
			}
		}
		time.Sleep(time.Second)
	}
}
