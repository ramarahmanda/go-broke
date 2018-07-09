package broke

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type BrokerGooglePubSub struct {
	conn *pubsub.Client
}

const (
	E_PUBSUB_MESSAGE_NOT_BYTE = "Message must be byte value"
	PUBSUB_PUBLISH_LIMIT      = 10
)

func (b *BrokerGooglePubSub) Publish(topic string, message interface{}) (interface{}, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
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
func (b *BrokerGooglePubSub) Subscribe(topic string, f func(msg interface{})) (interface{}, error) {
	ctx := context.Background()
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
			AckDeadline: 20 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}
	err = sub.Receive(ctx, func(c context.Context, msg *pubsub.Message) {
		f(msg.Data)
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (b *BrokerGooglePubSub) Close() {
	b.conn.Close()
}

func NewBrokerGooglePubSub() (Broker, error) {
	projectID, err := mustGetenv("GOOGLE_PROJECT_ID")
	if err != nil {
		return nil, err
	}
	credFilePath, err := mustGetenv("GOOGLE_CREDENTIAL")
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credFilePath))
	if err != nil {
		return nil, err
	}
	// Create topic if it doesn't exist.
	return &BrokerGooglePubSub{
		client,
	}, nil

}
