package broke

import (
	"errors"
	"fmt"
	"os"
)

const (
	E_BROKER_NOT_SUPPORTED = "Broker type not supported"
	E_ENV_NOTFOUND         = "ENV %s NOT FOUND"
)

const (
	BROKER_NATS          = "nats"
	BROKER_GOOGLE_PUBSUB = "gpubsub"
)

func mustGetenv(k string) (string, error) {
	v := os.Getenv(k)
	if v == "" {
		return "", errors.New(fmt.Sprintf(E_ENV_NOTFOUND, k))
	}
	return v, nil
}

type Broker interface {
	Publish(topic string, message interface{}) (interface{}, error)
	Subscribe(topic string, f func(msg interface{}) error) (interface{}, error)
	SubscribeWithOptions(topic string, f func(msg interface{}) error, options map[string]interface{}) (interface{}, error)
	Close()
}

func NewBroker(selectBroker string) (Broker, error) {
	switch selectBroker {
	case BROKER_NATS:
		return NewBrokerNats()
	case BROKER_GOOGLE_PUBSUB:
		return NewBrokerGooglePubSub()
	}
	return nil, errors.New(E_BROKER_NOT_SUPPORTED)
}
