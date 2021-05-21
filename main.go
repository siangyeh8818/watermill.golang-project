// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	stan "github.com/nats-io/stan.go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
)

var globalTopic = "example.topic"

func main() {

	//起一個訂閱者的client
	subscriber, err := nats.NewStreamingSubscriber(
		nats.StreamingSubscriberConfig{
			ClusterID:        "test-cluster",
			ClientID:         "example-subscriber",
			QueueGroup:       "example",
			DurableName:      "my-durable",
			SubscribersCount: 4, // how many goroutines should consume messages
			CloseTimeout:     time.Minute,
			AckWaitTimeout:   time.Second * 30,
			StanOptions: []stan.Option{
				stan.NatsURL("nats://127.0.0.1:4222"),
			},
			Unmarshaler: nats.GobMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}
	//訂閱者 所要訂的topic , message是一個chanel
	messages, err := subscriber.Subscribe(context.Background(), globalTopic)
	if err != nil {
		panic(err)
	}

	//用另一個go執行緒去接chanel回傳回來的訊息
	go process(messages)

	//建立一個新的發布者client
	publisher, err := nats.NewStreamingPublisher(
		nats.StreamingPublisherConfig{
			ClusterID: "test-cluster",
			ClientID:  "example-publisher",
			StanOptions: []stan.Option{
				stan.NatsURL("nats://127.0.0.1:4222"),
			},
			Marshaler: nats.GobMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	infinitepublishMessages(publisher, globalTopic)
}

func infinitepublishMessages(publisher message.Publisher, topic string) {

	//這是一個無窮迴圈的發送訊息的function
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish(topic, msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func publishMessages(publisher message.Publisher, topic string, pmessage []byte) {

	msg := message.NewMessage(watermill.NewUUID(), pmessage)

	if err := publisher.Publish(topic, msg); err != nil {
		panic(err)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
