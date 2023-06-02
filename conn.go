package s3selectsqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

type s3SelectConn struct {
	client   S3SelectClient
	cfg      *S3SelectConfig
	aliveCh  chan struct{}
	isClosed bool
}

func newConn(client S3SelectClient, cfg *S3SelectConfig) *s3SelectConn {
	return &s3SelectConn{
		client:  client,
		cfg:     cfg,
		aliveCh: make(chan struct{}),
	}
}

func (conn *s3SelectConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statment %w", ErrNotSupported)
}

func (conn *s3SelectConn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

func (conn *s3SelectConn) Close() error {
	if conn.isClosed {
		return nil
	}
	conn.isClosed = true
	close(conn.aliveCh)
	return nil
}

func (conn *s3SelectConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, fmt.Errorf("transaction %w", ErrNotSupported)
}

func (conn *s3SelectConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

type contentInfo struct {
	BucketName string
	ObjectKey  string
}

func (conn *s3SelectConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	debugLogger.Printf("query: %s", query)
	if conn.isClosed {
		return nil, sql.ErrConnDone
	}
	eg, egctx := errgroup.WithContext(ctx)
	contentCh := make(chan contentInfo, 100)
	pr, pw := io.Pipe()

	eg.Go(func() error {
		defer pw.Close()
		if err := conn.s3SelectWorker(egctx, query, args, contentCh, pw); err != nil {
			return err
		}
		return nil
	})
	rows := make([][]string, 0)
	eg.Go(func() error {
		reader := csv.NewReader(pr)
		for {
			select {
			case <-egctx.Done():
				return nil
			default:
			}
			row, err := reader.Read()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
	})

	if conn.cfg.ObjectKey != "" {
		eg.Go(func() error {
			defer close(contentCh)
			contentCh <- contentInfo{
				BucketName: conn.cfg.BucketName,
				ObjectKey:  conn.cfg.ObjectKey,
			}
			return nil
		})
	} else {
		eg.Go(func() error {
			defer close(contentCh)
			p := s3.NewListObjectsV2Paginator(conn.client, &s3.ListObjectsV2Input{
				Bucket:    aws.String(conn.cfg.BucketName),
				Prefix:    aws.String(conn.cfg.ObjectKeyPrefix),
				Delimiter: aws.String("/"),
			})
			for p.HasMorePages() {
				select {
				case <-conn.aliveCh:
					return sql.ErrConnDone
				default:
				}
				output, err := p.NextPage(ctx)
				if err != nil {
					return err
				}
				for _, content := range output.Contents {
					contentCh <- contentInfo{
						BucketName: *output.Name,
						ObjectKey:  *content.Key,
					}
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	debugLogger.Printf("complete s3 select: rows=%d", len(rows))
	return newRows(rows), nil
}

func (conn *s3SelectConn) s3SelectWorker(ctx context.Context, query string, args []driver.NamedValue, contentCh <-chan contentInfo, w io.Writer) error {
	for content := range contentCh {
		select {
		case <-conn.aliveCh:
			return sql.ErrConnDone
		default:
		}
		inputSerialization, err := conn.cfg.newInputSeliarization()
		if err != nil {
			return err
		}
		input := &s3.SelectObjectContentInput{
			Bucket:         aws.String(content.BucketName),
			Key:            aws.String(content.ObjectKey),
			Expression:     aws.String(query),
			ExpressionType: types.ExpressionTypeSql,
			OutputSerialization: &types.OutputSerialization{
				CSV: &types.CSVOutput{
					FieldDelimiter:  aws.String(","),
					RecordDelimiter: aws.String("\n"),
					QuoteFields:     types.QuoteFieldsAsneeded,
				},
			},
			InputSerialization: inputSerialization,
		}
		debugLogger.Printf("s3 select key=%s", content.ObjectKey)
		if err := conn.client.SelectObjectContentWithWriter(ctx, w, input); err != nil {
			return err
		}
	}
	return nil
}

func (conn *s3SelectConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("exec %w", ErrNotSupported)
}
