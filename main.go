package meetup

import (
	"context"
	"database/sql"
	"github.com/fjmendes1994/meetup/worker"
	"github.com/go-kit/kit/log"
	"gocloud.dev/pubsub"
	"net/http"
	"os"
	"time"
)

func main() {
	ctx := context.Background()

	logger := initLogger()

	httpClient := initHttpClient()

	psqlClient, err := initPsqlClient()
	if err != nil {
		panic(err)
	}

	topic := initSQSClient(err, ctx)
	if err != nil {
		panic(err)
	}

	w := worker.Worker{
		Logger:     logger,
		HttpClient: httpClient,
		PsqlClient: psqlClient,
		SQSClient:  topic,
	}

	err = w.Process(ctx, "LTC")
	if err != nil {
		panic(err)
	}

}

func initSQSClient(err error, ctx context.Context) *pubsub.Topic {
	topic, err := pubsub.OpenTopic(ctx, os.Getenv("SQS_URL"))
	return topic
}

func initPsqlClient() (*sql.DB, error) {
	psqlClient, err := sql.Open("postgres", os.Getenv("PSQL_CONN"))
	return psqlClient, err
}

func initHttpClient() *http.Client {
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	return httpClient
}

func initLogger() log.Logger {
	writer := log.NewSyncWriter(os.Stderr)
	logger := log.NewJSONLogger(writer)
	return logger
}
