package s3selectsqldriver

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3SelectClient interface {
	SelectObjectContentWithWriter(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error
	s3.ListObjectsV2APIClient
}

type S3SelectClientWithWriter struct {
	*s3.Client
}

func (c S3SelectClientWithWriter) SelectObjectContentWithWriter(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
	output, err := c.Client.SelectObjectContent(ctx, params, optFns...)
	if err != nil {
		return err
	}
	stream := output.GetStream()
	defer stream.Close()
	for event := range stream.Events() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, ok := event.(*types.SelectObjectContentEventStreamMemberRecords)
			if ok {
				_, err := w.Write(record.Value.Payload)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var S3SelectClientConstructor func(ctx context.Context, cfg *S3SelectConfig) (S3SelectClient, error)

func newS3SelectClient(ctx context.Context, cfg *S3SelectConfig) (S3SelectClient, error) {
	if S3SelectClientConstructor != nil {
		return S3SelectClientConstructor(ctx, cfg)
	}
	return DefaultS3SelectClientConstructor(ctx, cfg)
}

func DefaultS3SelectClientConstructor(ctx context.Context, cfg *S3SelectConfig) (S3SelectClient, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, cfg.S3OptFns...)
	return S3SelectClientWithWriter{
		Client: client,
	}, nil
}
