package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	redisPool            *redis.Pool
	nameConn             redis.Conn
	DEFAULT_REDIS_HOST   = "127.0.0.1:6379"
	DEFAULT_REDIS_PWD    = ""
	DEFAULT_SERVICE_NAME = "anonymous"
)

const (
	SERVICE_NAME_FORMAT = "service-%s"
)

//服务的初始化
//参数:
//    * redis_host		redis的地址
//    * redis_pwd		redis的密码
//    * service_name	服务名称
func Setup(redis_host, redis_pwd, service_name string) {
	host := DEFAULT_REDIS_HOST
	if redis_host != "" {
		host = redis_host
	}

	pwd := DEFAULT_REDIS_PWD
	if redis_pwd != "" {
		pwd = redis_pwd
	}

	name := DEFAULT_SERVICE_NAME
	if service_name != "" {
		name = service_name
	}

	redisPool = &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 10 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialPassword(pwd))
			if err != nil {
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}

			_, err := c.Do("PING")
			return err
		},
	}

	nameConn = redisPool.Get()
	_, err := nameConn.Do("client", "setname", fmt.Sprintf(SERVICE_NAME_FORMAT, name))
	if err != nil {
		panic("register service failed:" + err.Error())
	}
}

func Cleanup() {
	if nameConn != nil {
		nameConn.Close()
		nameConn = nil
	}

	if redisPool != nil {
		redisPool.Close()
		redisPool = nil
	}
}

func getRedisConn() redis.Conn {
	if redisPool == nil {
		panic("service not registered!")
	}

	return redisPool.Get()
}

type Service struct {
	Name    string
	Address string
	Age     int
}

//列出所有的服务,用于服务管理
func List() ([]Service, error) {
	conn := getRedisConn()
	defer conn.Close()

	reply, err := redis.String(conn.Do("client", "list"))
	if err != nil {
		return nil, err
	}

	reply = strings.TrimSuffix(reply, "\n")

	clients := strings.Split(reply, "\n")

	services := make([]Service, len(clients))
	length := 0

	for index, _ := range clients {
		client := string(clients[index])

		attrs := strings.Split(client, " ")
		if len(attrs) < 5 {
			continue
		}

		attrAddr := attrs[1]
		attrName := attrs[3]
		attrArg := attrs[4]

		var addr, name string
		var age int

		fmt.Sscanf(attrAddr, "addr=%s", &addr)
		fmt.Sscanf(attrName, "name="+SERVICE_NAME_FORMAT, &name)
		fmt.Sscanf(attrArg, "age=%d", &age)

		if name == "" {
			continue
		}

		services[length].Address = addr
		services[length].Name = name
		services[length].Age = age

		length += 1
	}

	return services[:length], nil
}
