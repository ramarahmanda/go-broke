package broke

import (
	"errors"
)

const (
	E_BROKER_NOT_SUPPORTED = "Broker type not supported"
)
const (
	BROKER_NATS          = "nats"
	BROKER_GOOGLE_PUBSUB = "gpubsub"
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
	case BROKER_GOOGLE_PUBSUB:
		return NewBrokerGooglePubSub(parameters["google_project"], parameters["google_credentials"])
	}
	return nil, errors.New(E_BROKER_NOT_SUPPORTED)
}
