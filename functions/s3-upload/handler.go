package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/ellery44/data-loader/internal/dataloader"
	_ "github.com/lib/pq"
)

type handler struct {
	dl *dataloader.DataLoader
}

func (h *handler) handle(ctx context.Context, s3Event events.S3Event) (*Response, error) {
	var err error
	for _, record := range s3Event.Records {
		err = h.dl.LoadDataFileToRedshift(ctx, record.S3.Object.Key)
	}
	if err != nil {
		return nil, err
	}
	return &Response{Success: true}, nil
}
