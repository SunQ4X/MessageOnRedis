package service

import (
	"encoding/json"
	"fmt"
	"time"

	"context"

	"github.com/garyburd/redigo/redis"
)

const MESSAGE_FORMAT = "message-%s"

//消息
//当有对应消息发布时,所有订阅者都可以收到消息
type Message interface {
	MessageType() string
}

var (
	msgCancels map[string]context.CancelFunc
)

func init() {
	msgCancels = make(map[string]context.CancelFunc)
}

//订阅消息
//参数
//    * messageType	消息类型
//    * constructor	消息构造器
//    * handler		消息处理器
func SubscribeMessage(messageType string, constructor func() Message, handler func(Message)) {
	ctx, cancel := context.WithCancel(context.Background())

	channel := fmt.Sprintf(MESSAGE_FORMAT, messageType)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn := getRedisConn()
				psc := &redis.PubSubConn{conn}
				defer psc.Close()

				err := psc.Subscribe(channel)
				if err != nil {
					goto SLEEP
				}

				defer psc.Unsubscribe(channel)

				ctxChild, _ := context.WithCancel(ctx)
				msgChan := make(chan []byte, 1)

				go func() {
					defer close(msgChan)

					for {
						select {
						case <-ctxChild.Done():
							return
						default:
							switch v := psc.Receive().(type) {
							case error:
								return
							case redis.Message:
								msgChan <- v.Data
							}
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

	msgCancels[messageType] = cancel
}

//取消订阅
func UnsubscribeMessage(messageType string) {
	cancel, ok := msgCancels[messageType]
	if ok {
		cancel()
	}
}

//发布消息
func PublishMessage(e Message) error {
	channel := fmt.Sprintf(MESSAGE_FORMAT, e.MessageType())

	content, err := json.Marshal(e)
	if err != nil {
		return err
	}

	conn := getRedisConn()
	defer conn.Close()

	_, err = conn.Do("PUBLISH", channel, content)
	if err != nil {
		return err
	}

	return nil
}
