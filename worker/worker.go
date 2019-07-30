package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/go-kit/kit/log"
	_ "github.com/lib/pq"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
)

type Fetcher interface {
	Get(url string) (resp *http.Response, err error)
}

type Database interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Sender interface {
	Send(ctx context.Context, m *pubsub.Message) (err error)
}

type Worker struct {
	HttpClient Fetcher
	PsqlClient Database
	SQSClient  Sender
	Logger     log.Logger
}

type Price struct {
	Currency string
	Buy      string
	Sell     string
}

type Ticker struct {
	Buy  string
	Sell string
}

type Tick struct {
	Ticker Ticker
}

const (
	PriceApi    = "https://www.mercadobitcoin.net/api/%s/ticker/"
	InsertQuery = `INSERT INTO price (currency, buy, sell) VALUES ($1, $2, $3)`
)

func (w *Worker) Process(ctx context.Context, currency string) error {

	tick, err := w.fetchPrice(currency)
	if err != nil {
		return err
	}

	price := Price{
		Currency: currency,
		Buy:      tick.Ticker.Buy,
		Sell:     tick.Ticker.Sell,
	}

	err = w.savePrice(price)
	if err != nil {
		return err
	}

	err = w.sendPriceUpdateMsg(ctx, price)
	if err != nil {
		return err
	}

	w.Logger.Log("message", "Currency updated", "currency", currency)

	return nil

}

func (w *Worker) fetchPrice(currency string) (*Tick, error) {
	resp, err := w.HttpClient.Get(fmt.Sprintf(PriceApi, currency))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	tick := new(Tick)
	if err = json.Unmarshal(body, &tick); err != nil {
		return nil, err
	}
	return tick, err
}

func (w *Worker) savePrice(price Price) error {
	_, err := w.PsqlClient.Exec(InsertQuery, price.Currency, price.Buy, price.Sell)
	return err
}

func (w *Worker) sendPriceUpdateMsg(ctx context.Context, price Price) error {
	message, err := json.Marshal(price)
	if err != nil {
		return err
	}
	return w.SQSClient.Send(ctx, &pubsub.Message{
		Body: message,
	})
}
