package handler

import (
	"context"
	"quote-manager/external/orders"
	"quote-manager/external/quotes"
	"quote-manager/external/tickets"
	"quote-manager/rabbit"
	"quote-manager/routes"
	"quote-manager/services"
	"quote-manager/storage"
	"time"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type TicketHandler struct {
	ticketStore  storage.TicketStorage
	quoteService services.QuoteService
	quoteSender  rabbit.RabbitSender
}

func NewTicketHandler(
	ticketStore storage.TicketStorage, quoteService services.QuoteService, quoteSender rabbit.RabbitSender) TicketHandler {
	return TicketHandler{ticketStore: ticketStore, quoteService: quoteService, quoteSender: quoteSender}
}

func (h *TicketHandler) Handle(ctx context.Context) {
	for {
		ticketIds, err := h.ticketStore.GetTickets(ctx)
		if err != nil {
			logger.Errorln(err.Error())
		}
		for _, ticketId := range ticketIds {
			ticketInfo, err := h.ticketStore.GetTicketById(ctx, ticketId)
			if err != nil {
				logger.Errorln(err.Error())
				continue
			}
			if ticketInfo.State == tickets.TicketState_TICKET_STATE_PROCESSING || ticketInfo.State == tickets.TicketState_TICKET_STATE_DONE {
				continue
			}
			ticketInfo.State = tickets.TicketState_TICKET_STATE_PROCESSING
			if err = h.ticketStore.UpdateTicket(ctx, ticketInfo); err != nil {
				logger.Errorln(err.Error())
				continue
			}

			switch ticketInfo.OperationType {
			case tickets.OperationType_OPERATION_TYPE_ORDER_INFO:
				request := &orders.OrderInfo{}
				if err := proto.Unmarshal(ticketInfo.Data, request); err != nil {
					logger.Errorln(err.Error())
					continue
				}
				go h.quoteService.UpdateMarket(ctx, request)
			case tickets.OperationType_OPERATION_TYPE_SEND_QUOTES:
				request := &quotes.MarketDeepthResponse{}
				if err := proto.Unmarshal(ticketInfo.Data, request); err != nil {
					logger.Errorln(err.Error())
					continue
				}
				go h.quoteSender.SendMessage(ctx, "e.quotes.forward", routes.RkQuoteInfo, request)
			default:
				logger.Warningln("Ticket operation ", ticketInfo.OperationType, " unsupported, skipping...")
				continue
			}

			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
	}

}
