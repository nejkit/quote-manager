package services

import (
	"context"
	"quote-manager/errors"
	"quote-manager/external/quotes"
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

func (q *QuoteService) UpdateMarket(ctx context.Context, request *quotes.UpdateQuoteRequest) error {
	currentDeepth, err := q.quoteStorage.GetDeepth(ctx, request.CurrencyPair, request.Direction, request.Price)
	if err != nil {
		return err
	}
	if err == errors.ErrorNotFound {
		currentDeepth = &storage.DeepthModel{
			CurrencyPair: request.CurrencyPair,
			Direction:    request.Direction,
		}
	}

	if request.OperationType == quotes.QuoteOperationType_QUOTE_OPERATION_TYPE_REMOVE_ORDER {
		request.Volume *= -1
	}
	currentDeepth.Volume += request.Volume
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

	if request.GetFilledPrice() != 0 {
		if err := q.updateQuote(ctx, request); err != nil {
			return err
		}
	}
	return nil
}

func (q *QuoteService) updateQuote(ctx context.Context, request *quotes.UpdateQuoteRequest) error {
	currentQuote, err := q.getQuote(ctx, request)
	if err != nil {
		logger.Errorln(err.Error())
		return err
	}
	if currentQuote.Nonce > uint64(request.Nonce) {
		return errors.ErrorNonceExpired
	}
	for {
		if err := q.quoteStorage.TryLockQuote(ctx, *currentQuote); err != nil {
			time.Sleep(time.Millisecond * 1)
			continue
		}
		break
	}

	currentQuote, err = q.getQuote(ctx, request)

	if currentQuote.Nonce > uint64(request.Nonce) {
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

func (q *QuoteService) getQuote(ctx context.Context, request *quotes.UpdateQuoteRequest) (*storage.QuoteModel, error) {
	currentQuote, err := q.quoteStorage.GetQuote(ctx, request.CurrencyPair, request.Direction)
	if err != nil {
		return nil, err
	}
	if err == errors.ErrorNotFound {
		currentQuote = &storage.QuoteModel{
			CurrencyPair: request.CurrencyPair,
			Direction:    request.Direction,
			Nonce:        uint64(request.Nonce),
		}
	}

	return currentQuote, nil

}

func (q *QuoteService) GetInfoAboutMarket(ctx context.Context, request *quotes.MarketDeepthRequest) {
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

}
