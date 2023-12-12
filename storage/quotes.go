package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"quote-manager/errors"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	QuoteHash  = "quotes"
	DeepthHash = "deepth"
)

type QuoteModel struct {
	CurrencyPair string
	Direction    int32
	LastPrice    float64
	LastVolume   float64
	Nonce        uint64
}

type DeepthModel struct {
	CurrencyPair string
	Direction    int32
	Price        float64
	Volume       float64
}

type QuoteManager struct {
	client     RedisClient
	instanceId string
}

func NewQuoteManager(client RedisClient) QuoteManager {
	return QuoteManager{client: client, instanceId: uuid.NewString()}
}

func (q *QuoteManager) SetQuote(ctx context.Context, quoteModel QuoteModel) error {
	bytes, err := json.Marshal(&quoteModel)
	if err != nil {
		return err
	}

	if err := q.client.InsertHash(ctx, QuoteHash, fmt.Sprintf("%s:%v", quoteModel.CurrencyPair, quoteModel.Direction), bytes); err != nil {
		return err
	}
	return nil
}

func (q *QuoteManager) SetDeepth(ctx context.Context, deepthModel DeepthModel) error {
	bytes, err := json.Marshal(&deepthModel)
	if err != nil {
		return err
	}

	if err := q.client.InsertHash(ctx, DeepthHash, buildKeyForDeepth(deepthModel.CurrencyPair, deepthModel.Direction, deepthModel.Price), bytes); err != nil {
		return err
	}
	return nil
}

func (q *QuoteManager) GetQuote(ctx context.Context, curPair string, direction int32) (*QuoteModel, error) {
	result, err := q.client.GetFromHash(ctx, QuoteHash, fmt.Sprintf("%s:%v", curPair, direction))
	if err != nil {
		return nil, err
	}
	var quoteModel QuoteModel
	if err = json.Unmarshal([]byte(*result), quoteModel); err != nil {
		return nil, err
	}
	return &quoteModel, nil

}

func (q *QuoteManager) GetDeepth(ctx context.Context, curPair string, direction int32, price float64) (*DeepthModel, error) {
	result, err := q.client.GetFromHash(ctx, DeepthHash, buildKeyForDeepth(curPair, direction, price))
	if err != nil {
		logrus.Infoln(err.Error(), buildKeyForDeepth(curPair, direction, price))
		return nil, err
	}
	var deepthModel DeepthModel
	if err = json.Unmarshal([]byte(*result), &deepthModel); err != nil {
		return nil, err
	}
	return &deepthModel, nil
}

func (q *QuoteManager) TryLockQuote(ctx context.Context, quoteInfo QuoteModel) error {
	result, err := q.client.SetKeyNX(ctx, fmt.Sprintf("lock_quote_%s:%v", quoteInfo.CurrencyPair, quoteInfo.Direction), q.instanceId)
	if err != nil {
		return err
	}
	if !result {
		return errors.ErrorBlockedResource
	}
	return nil
}

func (q *QuoteManager) TryLockDeepth(ctx context.Context, deepthInfo DeepthModel) error {
	result, err := q.client.SetKeyNX(ctx, "lock_deepth:"+buildKeyForDeepth(deepthInfo.CurrencyPair, deepthInfo.Direction, deepthInfo.Price), q.instanceId)
	if err != nil {
		return err
	}
	if !result {
		return errors.ErrorBlockedResource
	}
	return nil
}

func (q *QuoteManager) TryUnLockQuote(ctx context.Context, quoteInfo QuoteModel) error {
	value, err := q.client.GetKey(ctx, fmt.Sprintf("lock_quote_%s:%v", quoteInfo.CurrencyPair, quoteInfo.Direction))
	if value != q.instanceId {
		return nil
	}
	if err == errors.ErrorNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

func (q *QuoteManager) TryUnLockDeepth(ctx context.Context, deepthInfo DeepthModel) error {
	value, err := q.client.GetKey(ctx, "lock_deepth:"+buildKeyForDeepth(deepthInfo.CurrencyPair, deepthInfo.Direction, deepthInfo.Price))
	if value != q.instanceId {
		logrus.Errorln("Instance id not match: ", q.instanceId, value)
		return nil
	}
	if err == errors.ErrorNotFound {
		logrus.Errorln("NotExistsBlock: ", "lock_deepth:"+buildKeyForDeepth(deepthInfo.CurrencyPair, deepthInfo.Direction, deepthInfo.Price))
		return nil
	}
	if err != nil {
		logrus.Errorln(err.Error())
		return err
	}
	logrus.Infoln("Start delete block")
	q.client.DelKey(ctx, "lock_deepth:"+buildKeyForDeepth(deepthInfo.CurrencyPair, deepthInfo.Direction, deepthInfo.Price))
	return nil
}

func (q *QuoteManager) GetQuotes(ctx context.Context) ([]QuoteModel, []DeepthModel, error) {
	quotesFromRedis, err := q.client.GetAllFromHash(ctx, QuoteHash)
	if err != nil {
		return nil, nil, err
	}
	deepthFromRedis, err := q.client.GetAllFromHash(ctx, DeepthHash)
	if err != nil {
		return nil, nil, err
	}
	var quoteModels []QuoteModel
	for _, quote := range quotesFromRedis {
		quoteModel := QuoteModel{}
		if err = json.Unmarshal([]byte(quote), &quoteModel); err != nil {
			continue
		}
		quoteModels = append(quoteModels, quoteModel)
	}
	var deepthModels []DeepthModel
	for _, deepth := range deepthFromRedis {
		deepthModel := DeepthModel{}
		if err = json.Unmarshal([]byte(deepth), &deepthModel); err != nil {
			continue
		}
		deepthModels = append(deepthModels, deepthModel)
	}
	return quoteModels, deepthModels, nil
}

func buildKeyForDeepth(curPair string, direction int32, price float64) string {
	strPrice := fmt.Sprintf("%.*f", 2, price)
	return curPair + ":" + fmt.Sprintf("%d", direction) + ":" + strPrice
}
