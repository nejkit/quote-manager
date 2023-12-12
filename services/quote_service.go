package services

import (
	"context"
	"quote-manager/errors"
	"quote-manager/external/orders"
	"quote-manager/external/quotes"
	"quote-manager/external/tickets"
	"quote-manager/storage"
	"quote-manager/util"
	"time"

	logger "github.com/sirupsen/logrus"
)

type QuoteService struct {
	quoteStorage  storage.QuoteManager
	ticketStorage storage.TicketStorage
}

func NewQuoteService(quoteStorage storage.QuoteManager) QuoteService {
	return QuoteService{quoteStorage: quoteStorage}
}

func (q *QuoteService) UpdateMarket(ctx context.Context, request *orders.OrderInfo) error {

	currentDeepth, err := q.quoteStorage.GetDeepth(ctx, request.CurrencyPair, int32(request.Direction), request.InitPrice)
	if err != nil && err != errors.ErrorNotFound {
		return err
	}
	if err == errors.ErrorNotFound {
		currentDeepth = &storage.DeepthModel{
			CurrencyPair: request.CurrencyPair,
			Direction:    int32(request.Direction),
		}
	}

	changeVolume := 0.0
	changeVolume = request.InitVolume - request.FillVolume
	changeVolume *= -1

	if request.OrderState == orders.OrderState_ORDER_STATE_IN_PROCESS {
		changeVolume = request.InitVolume
	}

	currentDeepth.Volume += request.InitVolume
	for {
		if err := q.quoteStorage.TryLockDeepth(ctx, *currentDeepth); err != nil {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if err := q.quoteStorage.SetDeepth(ctx, *currentDeepth); err != nil {
			q.quoteStorage.TryUnLockDeepth(ctx, *currentDeepth)
			logger.Errorln(err.Error())
			return err
		}
		q.quoteStorage.TryUnLockDeepth(ctx, *currentDeepth)
		break
	}

	return nil
}

func (q *QuoteService) updateQuote(ctx context.Context, request *orders.OrderInfo) error {
	currentQuote, err := q.getQuote(ctx, request)
	if err != nil {
		logger.Errorln(err.Error())
		return err
	}

	for {
		if err := q.quoteStorage.TryLockQuote(ctx, *currentQuote); err != nil {
			time.Sleep(time.Millisecond * 1)
			continue
		}
		break
	}

	currentQuote, err = q.getQuote(ctx, request)

	if currentQuote.Nonce > uint64(request.Date.AsTime().UTC().UnixMilli()) {
		q.quoteStorage.TryUnLockQuote(ctx, *currentQuote)
		return errors.ErrorNonceExpired
	}

	if err := q.quoteStorage.SetQuote(ctx, *currentQuote); err != nil {
		logger.Errorln(err.Error())
		q.quoteStorage.TryUnLockQuote(ctx, *currentQuote)
		return err
	}
	q.quoteStorage.TryUnLockQuote(ctx, *currentQuote)
	return nil

}

func (q *QuoteService) getQuote(ctx context.Context, request *orders.OrderInfo) (*storage.QuoteModel, error) {
	currentQuote, err := q.quoteStorage.GetQuote(ctx, request.CurrencyPair, int32(request.Direction))
	if err != nil && err != errors.ErrorNotFound {
		return nil, err
	}
	if err == errors.ErrorNotFound {
		currentQuote = &storage.QuoteModel{
			CurrencyPair: request.CurrencyPair,
			Direction:    int32(request.Direction),
			Nonce:        uint64(request.Date.AsTime().UTC().UnixMilli()),
		}
	}

	return currentQuote, nil

}

func (q *QuoteService) SendQuotesSheduller(ctx context.Context, request *quotes.MarketDeepthRequest) {
	for {
		quoteInfo, deepthInfo, err := q.quoteStorage.GetQuotes(ctx, request)
		if err != nil {
			return
		}
		quoteInfoProto := util.MapQuotesInfosToProto(quoteInfo)
		deepthInfoProto := util.MapDeepthInfoToProto(deepthInfo)
		response := quotes.MarketDeepthResponse{
			Id:                request.Id,
			MarketDeepthInfos: deepthInfoProto,
			QuotesInfos:       quoteInfoProto,
		}
		q.ticketStorage.SaveTicketForOperation(ctx, tickets.OperationType_OPERATION_TYPE_SEND_QUOTES, &response)
		time.Sleep(time.Second * 5)

		select {
		case <-ctx.Done():
			return
		default:
			continue
		}

	}
}
