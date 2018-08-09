package service

import (
	"encoding/json"
	"fmt"
	"time"

	"context"

	//	"github.com/garyburd/redigo/redis"
)

const EVENT_FORMAT = "event-%s"

//事件
//当有对应事件推送时,只有其中一个事件注册者可以收到事件
type Event interface {
	EventType() string
}

var (
	eventCancels map[string]context.CancelFunc
)

func init() {
	eventCancels = make(map[string]context.CancelFunc)
}

//注册事件
//参数
//    * eventType	事件类型
//    * constructor	事件构造器
//    * handler		事件处理器
func RegisterEvent(eventType string, constructor func() Event, handler func(Event)) {
	ctx, cancel := context.WithCancel(context.Background())

	channel := fmt.Sprintf(EVENT_FORMAT, eventType)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ctxChild, _ := context.WithCancel(ctx)
				msgChan := make(chan []byte, 1)

				go func() {
					defer close(msgChan)

					conn := getRedisConn()
					defer conn.Close()

					for {
						select {
						case <-ctxChild.Done():
							return
						default:
							reply, err := conn.Do("brpop", channel, 0)
							if err != nil {
								return
							}

							msgChan <- reply.([]interface{})[1].([]byte)
						}
					}
				}()

				for {
					select {
					case <-ctx.Done():
						return
					case message, ok := <-msgChan:
						if !ok {
							goto SLEEP
						}

						e := constructor()
						err := json.Unmarshal(message, e)
						if err != nil {
							continue
						}

						handler(e)
					}
				}
			}

		SLEEP:
			time.Sleep(1 * time.Second)
		}
	}()

	eventCancels[eventType] = cancel
}

//取消注册
func UnregisterEvent(eventType string) {
	cancel, ok := eventCancels[eventType]
	if ok {
		cancel()
	}
}

//推送事件
func PostEvent(e Event) error {
	channel := fmt.Sprintf(EVENT_FORMAT, e.EventType())

	content, err := json.Marshal(e)
	if err != nil {
		return err
	}

	conn := getRedisConn()
	defer conn.Close()

	_, err = conn.Do("lpush", channel, content)
	if err != nil {
		return err
	}

	return nil
}
