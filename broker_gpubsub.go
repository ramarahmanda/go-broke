package broke

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"google.golang.org/api/option"
	"os"
	"time"
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
	return b.SubscribeWithOptions(topic, f, nil)
}

func (b *BrokerGooglePubSub) SubscribeWithOptions(topic string, f func(msg interface{}) error, options map[string]interface{}) (interface{}, error) {
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
	isPushSubscription := false
	if !exist {
		config := pubsub.SubscriptionConfig{
			Topic:       pubTopic,
			AckDeadline: 10 * time.Second,
		}

		subName := topic
		if val, exists := options["name"]; exists {
			if str, isString := val.(string); isString {
				subName = str
			}
		}

		if val, exists := options["push_config"]; exists {
			if ps, isPushConfig := val.(pubsub.PushConfig); isPushConfig {
				config.PushConfig = ps
				isPushSubscription = true
			}
		}

		sub, err = b.conn.CreateSubscription(ctx, subName, config)
		if err != nil {
			return nil, err
		}
	}

	if isPushSubscription {
		return nil, nil
	}

	cctx, cancel := context.WithCancel(context.Background())
	go sub.Receive(cctx, func(c context.Context, msg *pubsub.Message) {
		if err := f(msg.Data); err != nil {
			msg.Nack()
			return
		}
		msg.Ack()
	})
	return cctx, nil
}

func (b *BrokerGooglePubSub) Close() {
	b.conn.Close()
}
