package broke

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type GooglePubsubOptions struct {
	TimeoutSeconds time.Duration
}
type BrokerGooglePubSub struct {
	conn    *pubsub.Client
	Options GooglePubsubOptions
}

const (
	E_PUBSUB_MESSAGE_NOT_BYTE = "Message must be byte value"
	PUBSUB_PUBLISH_LIMIT      = 20
)

func (b *BrokerGooglePubSub) Publish(topic string, message interface{}) (interface{}, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*b.Options.TimeoutSeconds)
	defer cancel()
	var result *pubsub.PublishResult
	pubTopic := b.conn.Topic(topic)
	exist, err := pubTopic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exist {
		pubTopic, err = b.conn.CreateTopic(ctx, topic)
		if err != nil {
			return nil, err
		}
	}
	result = pubTopic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	_, err = result.Get(ctx)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
func (b *BrokerGooglePubSub) Subscribe(topic string, f func(msg interface{}) error) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*b.Options.TimeoutSeconds)
	defer cancel()
	pubTopic := b.conn.Topic(topic)
	exist, err := pubTopic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exist {
		pubTopic, err = b.conn.CreateTopic(ctx, topic)
		if err != nil {
			return nil, err
		}
	}
	sub := b.conn.Subscription(topic)
	exist, err = sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exist {
		sub, err = b.conn.CreateSubscription(ctx, topic, pubsub.SubscriptionConfig{
			Topic:       pubTopic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}
	var mu sync.Mutex
	cctx, cancel := context.WithCancel(context.Background())
	received := 0
	go sub.Receive(cctx, func(c context.Context, msg *pubsub.Message) {
		if err := f(msg.Data); err != nil {
			msg.Nack()
			return
		}
		mu.Lock()
		defer mu.Unlock()
		received++
		msg.Ack()
		if received == 10 {
			cancel()
		}
	})
	return nil, nil
}

func (b *BrokerGooglePubSub) Close() {
	b.conn.Close()
}

func NewBrokerGooglePubSub() (Broker, error) {
	var options = GooglePubsubOptions{
		TimeoutSeconds: 10,
	}
	if timeoutEnv := os.Getenv("TIMEOUT_SECONDS"); timeoutEnv != "" {
		timeoutDuration, err := time.ParseDuration(timeoutEnv)
		if err == nil {
			options.TimeoutSeconds = timeoutDuration
		}
	}
	projectID, err := mustGetenv("GOOGLE_PROJECT_ID")
	if err != nil {
		return nil, err
	}
	credFilePath, err := mustGetenv("GOOGLE_CREDENTIAL")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*options.TimeoutSeconds)
	defer cancel()

	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credFilePath))
	if err != nil {
		return nil, err
	}
	// Create topic if it doesn't exist.
	return &BrokerGooglePubSub{
		client,
		options,
	}, nil

}
