package rabbit

import (
	"errors"
	"quote-manager/routes"
	"time"

	"github.com/rabbitmq/amqp091-go"
	logger "github.com/sirupsen/logrus"
)

type RabbitFactory struct {
	connection *amqp091.Connection
}

func NewRabbitFactory(connectionString string) (*RabbitFactory, error) {
	con, err := tryConnectToRmq(connectionString)
	if err != nil {
		return nil, err
	}

	return &RabbitFactory{connection: con}, nil

}

func (f *RabbitFactory) Init() error {
	ch, err := f.GetRmqChannel()
	if err != nil {
		return err
	}
	defer ch.Close()
	ch.ExchangeDeclare("e.quotes.forward", "topic", true, false, false, false, nil)
	ch.QueueDeclare(routes.QueueQuoteInfos, true, false, false, false, nil)
	ch.QueueDeclare(routes.QueueUpdateQuotes, true, false, false, false, nil)
	ch.QueueBind(routes.QueueQuoteInfos, routes.RkQuoteInfo, "e.quotes.forward", true, nil)
	ch.QueueBind(routes.QueueUpdateQuotes, routes.RkOrderInfo, routes.ExNameOrders, true, nil)
	return nil
}

func (f *RabbitFactory) GetRmqChannel() (*amqp091.Channel, error) {
	channel, err := f.connection.Channel()
	if err != nil {
		return nil, err
	}
	return channel, nil
}

func (f *RabbitFactory) GetSender() (*RabbitSender, error) {
	channel, err := f.GetRmqChannel()
	if err != nil {
		return nil, err
	}
	return &RabbitSender{channelRmq: channel}, nil
}

func tryConnectToRmq(connectionString string) (*amqp091.Connection, error) {
	for i := 0; i < 60; i++ {
		con, err := amqp091.Dial(connectionString)
		if err != nil {
			logger.Errorln("Fail connect to rmq by address: ", connectionString, " Attempt: ", i)
			time.Sleep(1 * time.Second)
		}

		return con, nil
	}
	return nil, errors.New("RabbitUnvailable")
}
