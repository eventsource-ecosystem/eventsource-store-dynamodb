package lib

import (
	"context"

	"github.com/eventsource-ecosystem/eventsource"
)

type Producer func(ctx context.Context, aggregateID string, records []eventsource.Record) error

type ProducerSlice []Producer

func (p ProducerSlice) Produce(ctx context.Context, aggregateID string, records []eventsource.Record) error {
	for _, fn := range p {
		if err := fn(ctx, aggregateID, records); err != nil {
			return err
		}
	}

	return nil
}
