package main

import (
	"github.com/17cargo/sredis"
)

func main() {
	redisClient := new(sredis.SRedis)
	if err := redisClient.OpenWithConfig(sredis.RedisConfig{
		Addr: "127.0.0.1:6379",
	}); err == nil {
		redisClient.Do("PING")
	}
}
