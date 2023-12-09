package rabbit

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
	logger "github.com/sirupsen/logrus"
)

type IRabbitFactory interface {
	GetRmqChannel() (*amqp091.Channel, error)
}

type IProcessor[T any] interface {
	ProcessMessage(ctx context.Context, msg amqp091.Delivery)
}

type Listener[T any] struct {
	processor   IProcessor[T]
	channelAmqp *amqp091.Channel
	messageChan <-chan amqp091.Delivery
}

func NewListener[T any](ctx context.Context, factory IRabbitFactory, processor IProcessor[T], queueName string) (*Listener[T], error) {
	channel, err := factory.GetRmqChannel()
	if err != nil {
		return nil, err
	}

	messageChannel, err := channel.ConsumeWithContext(
		ctx,
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	return &Listener[T]{
		processor:   processor,
		channelAmqp: channel,
		messageChan: messageChannel,
	}, nil
}

func (l *Listener[T]) Run(ctx context.Context) {
	for {
		select {
		case msg, ok := <-l.messageChan:
			if !ok {
				logger.Errorln("Failed consume message, skipping...")
				continue
			}
			go l.processor.ProcessMessage(ctx, msg)
		default:
			time.Sleep(1 * time.Millisecond)
		}

	}
}
