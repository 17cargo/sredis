package sredis

import (
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

type RedisConfig struct {
	Addr string `ini:"addr"`
	DB   int    `ini:"index"`
	Pwd  string `ini:"pwd"`
	Size int    `ini:"size"`
}

type SRedis struct {
	client *RoundRobinAsyncClient
}

func (r *SRedis) OpenWithConfig(config RedisConfig) error {
	dial := func() (redigo.Conn, error) {
		var options = []redigo.DialOption{
			redigo.DialConnectTimeout(1 * time.Second),
			redigo.DialWriteTimeout(1 * time.Second),
			redigo.DialReadTimeout(1 * time.Second),
		}
		if config.DB != 0 {
			options = append(options, redigo.DialDatabase(config.DB))
		}
		if len(config.Pwd) > 0 {
			options = append(options, redigo.DialPassword(config.Pwd))
		}

		return redigo.Dial("tcp", config.Addr, options...)
	}
	if config.Size <= 0 {
		config.Size = 1
	}
	r.client = NewRoundRobinAsyncClient(dial, SetRoundRobinAsyncClientConnectionCount(config.Size))
	return nil
}

func (r *SRedis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return r.client.AsyncDo(commandName, args...)
}
