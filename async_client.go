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
}

func NewRoundRobinAsyncClient(newFn func() (redigo.Conn, error), connectCnt int) *RoundRobinAsyncClient {
	ret := &RoundRobinAsyncClient{
		Dial:       newFn,
		connectCnt: connectCnt,
		clients:    make([]*Client, 0),
	}
	ret.init()
	return ret
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
		cli.clients[index].ResetConn(c)
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
