package orchestrator

import (
	"os"
	"strconv"
)

type Config struct {
	RedisIP       string
	RedisPort     int
	RedisPassword string
	RedisDB       int
}

func NewConfigFromEnv() Config {
	redisPort, err := strconv.ParseInt(os.Getenv("REDIS_PORT"), 10, 32)
	if err != nil {
		redisPort = 6379
	}
	redisDB, err := strconv.ParseInt(os.Getenv("REDIS_DB"), 10, 32)
	if err != nil {
		redisDB = 0
	}
	return Config{
		RedisIP:       os.Getenv("REDIS_IP"),
		RedisPort:     int(redisPort),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       int(redisDB),
	}
}
