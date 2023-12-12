package handler

import (
	"context"
	"quote-manager/external/orders"
	"quote-manager/external/tickets"
	"quote-manager/storage"
)

type Handler struct {
	ticketStore storage.TicketStorage
}

func NewHandler(ticketStore storage.TicketStorage) Handler {
	return Handler{ticketStore: ticketStore}
}

func (h *Handler) GetHandlerForUpdateQuotes() func(context.Context, *orders.OrderInfo) {
	return func(ctx context.Context, uqr *orders.OrderInfo) {

		if uqr.OrderState == orders.OrderState_ORDER_STATE_PART_FILL ||
			uqr.OrderState == orders.OrderState_ORDER_STATE_FILL ||
			uqr.OrderState == orders.OrderState_ORDER_STATE_NEW {
			return
		}

		h.ticketStore.SaveTicketForOperation(ctx, tickets.OperationType_OPERATION_TYPE_ORDER_INFO, uqr)
	}
}
