package util

import (
	"quote-manager/external/orders"
	"quote-manager/external/quotes"
	"quote-manager/storage"

	"google.golang.org/protobuf/proto"
)

func MapDeepthInfoToProto(infos []storage.DeepthModel) []*quotes.MarketDeepthInfo {
	data := make(map[string]map[int][]*quotes.PriceToVolumeInfo)
	for _, val := range infos {
		data[val.CurrencyPair][int(val.Direction)] = append(data[val.CurrencyPair][int(val.Direction)], &quotes.PriceToVolumeInfo{
			Price:  val.Price,
			Volume: val.Volume,
		})
	}
	result := make([]*quotes.MarketDeepthInfo, 0)
	for k, v := range data {
		result = append(result, &quotes.MarketDeepthInfo{
			CurrencyPair: k,
			BidInfo:      v[1],
			AskInfo:      v[2],
		})
	}
	return result

}

func MapQuotesInfosToProto(infos []storage.QuoteModel) []*quotes.QuotesInfo {

	result := make([]*quotes.QuotesInfo, 0)
	for _, val := range infos {
		result = append(result, &quotes.QuotesInfo{
			CurrencyPair: val.CurrencyPair,
			Direction:    val.Direction,
			FilledPrice:  val.LastPrice,
			FilledVolume: val.LastVolume,
		})
	}
	return result
}

func GetParserForUpdateQuotesRequest() func([]byte) (*orders.OrderInfo, error) {
	return func(b []byte) (*orders.OrderInfo, error) {
		var request *orders.OrderInfo
		err := proto.Unmarshal(b, request)
		if err != nil {
			return nil, err
		}
		return request, nil
	}
}
