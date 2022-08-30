package broker

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/google/wire"
	"go.uber.org/zap"
)

// BrokerProvider
var BrokerProvider = wire.NewSet(NewBroker)

// NewBroker
func NewBroker(ctx context.Context, log *zap.Logger, dsn string) (*Broker, error) {
	if dsn == "" {
		return nil, errors.New("broker dsn is empty")
	}
	uri, errParse := url.Parse(dsn)
	if errParse != nil {
		return nil, fmt.Errorf("dependency %s parse uri %s error for %w", "broker", dsn, errParse)
	}
	broker := &Broker{}
	err := broker.Init(ctx, log, uri)
	if err != nil {
		return nil, err
	}
	return broker, nil
}
