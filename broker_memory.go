package broke

import (
	"errors"
	"sync"
)

func NewBrokerMemory() (Broker, error) {
	b := BrokerMemory{
		contents:      make(chan brokerMessage),
		topics:        make(map[string]bool),
		subscriptions: make(map[string]map[string]func(msg interface{}) error),
		terminated:    make(chan bool),
	}

	go b.startListeningTopics()

	return &b, nil
}

type BrokerMemory struct {
	contents      chan brokerMessage
	topics        map[string]bool
	subscriptions map[string]map[string]func(msg interface{}) error
	terminated    chan bool
	writeLock     sync.Mutex
}

type brokerMessage struct {
	Topic string
	Data  interface{}
}

func (b *BrokerMemory) Close() {
	b.terminated <- true
}

func (b *BrokerMemory) Publish(topic string, message interface{}) (interface{}, error) {

	b.writeLock.Lock()
	b.topics[topic] = true
	b.writeLock.Unlock()

	b.contents <- brokerMessage{topic, message}

	return nil, nil
}

func (b *BrokerMemory) Subscribe(topic string, f func(msg interface{}) error) (interface{}, error) {
	return b.SubscribeWithOptions(topic, f, nil)
}

func (b *BrokerMemory) SubscribeWithOptions(topic string, f func(msg interface{}) error, options map[string]interface{}) (interface{}, error) {
	if b.isTerminated() {
		return nil, errors.New("topic listener has been stopped")
	}

	subName := topic

	if val, exists := options["name"]; exists {
		if str, isString := val.(string); isString {
			subName = str
		}
	}

	b.writeLock.Lock()
	defer b.writeLock.Unlock()
	if _, ok := b.subscriptions[topic]; !ok {
		b.subscriptions[topic] = make(map[string]func(msg interface{}) error)
	}

	b.subscriptions[topic][subName] = f

	return nil, nil
}

func (b *BrokerMemory) startListeningTopics() {
	for {
		select {
		case <-b.terminated:
			return
		case msg := <-b.contents:
			topicSubs := b.subscriptions[msg.Topic]
			for _, f := range topicSubs {
				go f(msg.Data)
			}
		}
	}
}

func (b *BrokerMemory) isTerminated() bool {
	return len(b.terminated) > 0
}
