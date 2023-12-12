package main

import (
	"context"
	"os"
	"os/signal"
	"quote-manager/external/orders"
	"quote-manager/external/quotes"
	"quote-manager/handler"
	"quote-manager/rabbit"
	"quote-manager/routes"
	"quote-manager/services"
	"quote-manager/storage"
	"quote-manager/util"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	ctxWithCancel, cancel := context.WithCancel(ctx)
	rmqFactory, err := rabbit.NewRabbitFactory("amqp://admin:admin@rabbitmq:5672")
	rmqFactory.Init()
	if err != nil {
		return
	}
	redisClient := storage.NewRedisClient("redis:6379")
	ticketStorage := storage.NewTicketStorage(redisClient)
	quoteStorage := storage.NewQuoteManager(redisClient)
	rmqSender, err := rmqFactory.GetSender()
	if err != nil {
		return
	}
	quoteService := services.NewQuoteService(quoteStorage)
	handlerToStorage := handler.NewHandler(ticketStorage)
	ticketHandler := handler.NewTicketHandler(ticketStorage, quoteService, *rmqSender)
	quoteProcessor := rabbit.NewProcessor[orders.OrderInfo](util.GetParserForUpdateQuotesRequest(), handlerToStorage.GetHandlerForUpdateQuotes())
	quoteListener, err := rabbit.NewListener[quotes.UpdateQuoteRequest](ctxWithCancel, rmqFactory, &quoteProcessor, routes.QueueUpdateQuotes)
	if err != nil {
		return
	}
	go ticketHandler.Handle(ctxWithCancel)
	go quoteListener.Run(ctxWithCancel)

	exit := make(chan os.Signal, 1)
	for {
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
		select {
		case <-exit:
			{
				cancel()
				return
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

}
