package s3selectsqldriver

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3SelectClient struct {
	SelectObjectContentWithWriterFunc func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error
	ListObjectsV2Func                 func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func (m *mockS3SelectClient) SelectObjectContentWithWriter(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
	if m.SelectObjectContentWithWriterFunc == nil {
		return errors.New("unexpected call SelectObjectContentWithWriter")
	}
	return m.SelectObjectContentWithWriterFunc(ctx, w, params)
}

func (m *mockS3SelectClient) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.ListObjectsV2Func == nil {
		return nil, errors.New("unexpected call ListObjectsV2")
	}
	return m.ListObjectsV2Func(ctx, params)
}
