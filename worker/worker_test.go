package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"gocloud.dev/pubsub"

	"github.com/golang/mock/gomock"
	_ "github.com/lib/pq"
	_ "gocloud.dev/pubsub/awssnssqs"
)

func TestWorkerProcess(t *testing.T) {

	type input struct {
		currency string
		resp     string
		price    Price
	}

	tests := []struct {
		name    string
		input   input
		wantErr bool
	}{
		{
			name: "with valid currency",
			input: input{
				currency: "Libra",
				resp:     `{"ticker": {"buy":"50.00","sell":"60.00"}}`,
				price: Price{
					Currency: "Libra",
					Buy:      "50.00",
					Sell:     "60.00",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ctrl, ctx := gomock.WithContext(context.Background(), t)

			defer ctrl.Finish()

			httpClient := NewMockFetcher(ctrl)
			psqlClient := NewMockDatabase(ctrl)
			sqsClient := NewMockSender(ctrl)
			logger := NewMockLogger(ctrl)

			w := &Worker{
				HttpClient: httpClient,
				PsqlClient: psqlClient,
				SQSClient:  sqsClient,
				Logger:     logger,
			}

			payload := ioutil.NopCloser(bytes.NewReader([]byte(tt.input.resp)))

			httpClient.EXPECT().
				Get(fmt.Sprintf(PriceApi, tt.input.currency)).
				Times(1).
				Return(&http.Response{
					Body: payload,
				}, nil)

			psqlClient.EXPECT().
				Exec(InsertQuery, tt.input.price.Currency, tt.input.price.Buy, tt.input.price.Sell).
				Times(1)

			message, err := json.Marshal(tt.input.price)
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.Process() error = %v, wantErr %v", err, tt.wantErr)
			}

			sqsClient.EXPECT().
				Send(ctx, &pubsub.Message{
					Body: message,
				}).
				Times(1)

			logger.EXPECT().
				Log("message", "Currency updated", "currency", tt.input.currency).
				Times(1)

			if err := w.Process(ctx, tt.input.currency); (err != nil) != tt.wantErr {
				t.Errorf("Worker.Process() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
