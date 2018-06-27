package broke

import (
	"errors"
)

const (
	E_BROKER_NOT_SUPPORTED = "Broker type not supported"
)
const (
	BROKER_NATS = "nats"
)

type Broker interface {
	Publish(topic string, message interface{}) (interface{}, error)
	Subscribe(topic string, f func(msg interface{})) (interface{}, error)
	Close()
}

func NewBroker(selectBroker string, parameters map[string]string) (Broker, error) {
	switch selectBroker {
	case BROKER_NATS:
		return NewBrokerNats(parameters["url"])
	}
	return nil, errors.New(E_BROKER_NOT_SUPPORTED)
}
