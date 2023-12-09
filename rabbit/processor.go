package rabbit

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type Parser[T any] func([]byte) (*T, error)
type Handler[T any] func(context.Context, *T)

type Processor[T any] struct {
	parser  Parser[T]
	handler Handler[T]
}

func NewProcessor[T any](parser Parser[T], handler Handler[T]) Processor[T] {
	return Processor[T]{
		parser:  parser,
		handler: handler,
	}
}

func (p *Processor[T]) ProcessMessage(ctx context.Context, msg amqp091.Delivery) {
	model, err := p.parser(msg.Body)
	if err != nil {
		msg.Nack(false, false)
		return
	}
	p.handler(ctx, model)
	msg.Ack(false)
}
