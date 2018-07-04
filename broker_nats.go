package broke

import (
	"encoding/json"

	"github.com/nats-io/go-nats"
)

type BrokerNats struct {
	conn *nats.Conn
}

const (
	E_NATS_MESSAGE_NOT_BYTE = "Message must be byte value"
)

func (b *BrokerNats) Publish(topic string, message interface{}) (interface{}, error) {
	msgByte, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	err = b.conn.Publish(topic, msgByte)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
func (b *BrokerNats) Subscribe(topic string, f func(msg interface{})) (interface{}, error) {
	return b.conn.Subscribe(topic, func(m *nats.Msg) {
		f(m.Data)
	})
}

func (b *BrokerNats) Close() {
	b.conn.Close()
}

func NewBrokerNats() (Broker, error) {
	natsUrl, err := mustGetenv("NATS_URL")
	if err != nil {
		return nil, err
	}
	natsCon, err := nats.Connect(natsUrl)
	if err != nil {
		return nil, err
	}
	return &BrokerNats{
		natsCon,
	}, nil
}
