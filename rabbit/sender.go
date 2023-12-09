package rabbit

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type RabbitSender struct {
	channelRmq *amqp091.Channel
}

func (s *RabbitSender) SendMessage(ctx context.Context, exchange string, rk string, message protoreflect.ProtoMessage) error {
	body, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if err = s.channelRmq.PublishWithContext(
		ctx,
		exchange,
		rk,
		false,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        body,
		}); err != nil {
		return err
	}
	return nil
}
