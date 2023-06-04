package s3selectsqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/iancoleman/orderedmap"
	"github.com/mashiike/s3-select-sql-driver/lexer"
	"github.com/samber/lo"
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
	if len(args) > 0 {
		var err error
		query, err = conn.rewriteQuery(query, args)
		if err != nil {
			return nil, err
		}
		debugLogger.Printf("rewrited query: %s", query)
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
	columns := make([]string, 0)
	rows := make([][]interface{}, 0)
	eg.Go(func() error {
		defer pr.Close()
		dec := json.NewDecoder(pr)
		for {
			select {
			case <-conn.aliveCh:
				return sql.ErrConnDone
			case <-egctx.Done():
				return nil
			default:
			}
			o := orderedmap.New()
			if err := dec.Decode(o); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			keys := o.Keys()
			for _, key := range keys {
				// containes key in columns check and if not contains then append
				if lo.Contains(columns, key) {
					continue
				}
				columns = append(columns, key)
			}
			row := make([]interface{}, 0, len(columns))
			for _, key := range columns {
				v, ok := o.Get(key)
				if !ok {
					row = append(row, nil)
					continue
				}
				switch v := v.(type) {
				case *orderedmap.OrderedMap:
					b, err := json.Marshal(v)
					if err != nil {
						return err
					}
					var nv interface{}
					if err := json.Unmarshal(b, &nv); err != nil {
						return err
					}
					row = append(row, nv)
				default:
					row = append(row, v)
				}
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
				case <-egctx.Done():
					return nil
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
	var parseTime bool
	if conn.cfg.ParseTime != nil {
		parseTime = *conn.cfg.ParseTime
	}
	return newRows(columns, rows, parseTime), nil
}

func (conn *s3SelectConn) s3SelectWorker(ctx context.Context, query string, args []driver.NamedValue, contentCh <-chan contentInfo, w io.Writer) error {
	for content := range contentCh {
		select {
		case <-conn.aliveCh:
			return sql.ErrConnDone
		case <-ctx.Done():
			return nil
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
				JSON: &types.JSONOutput{},
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

func (conn *s3SelectConn) rewriteQuery(query string, args []driver.NamedValue) (string, error) {
	l := lexer.NewLexer(query)
	tokens, err := l.Lex()
	if err != nil {
		return "", err
	}
	var isNamedArgs, isOrdinalArgs bool
	argsByName := make(map[string]driver.NamedValue)
	usedByName := make(map[string]bool, len(args))
	for _, arg := range args {
		if arg.Name == "" {
			isOrdinalArgs = true
			continue
		}
		isNamedArgs = true
		argsByName[":"+arg.Name] = arg
	}
	if isNamedArgs && isOrdinalArgs {
		return "", fmt.Errorf("cannot use both named and ordinal parameters")
	}
	var builder strings.Builder
	var i int
	for _, token := range tokens {
		switch token.Kind {
		case lexer.KindNamedPlaceholder:
			if !isNamedArgs {
				return "", errors.New("required named parameter, but ordinal parameter is given")
			}
			arg, ok := argsByName[token.Value]
			if !ok {
				return "", fmt.Errorf("missing named parameter: %s", strings.TrimPrefix(token.Value, ":"))
			}
			s, err := conn.convertNamedArgToString(arg)
			if err != nil {
				return "", err
			}
			usedByName[token.Value] = true
			builder.WriteString(s)
		case lexer.KindPlaceholder:
			if !isOrdinalArgs {
				return "", errors.New("required ordinal parameter, but named parameter is given")
			}
			if i >= len(args) {
				return "", fmt.Errorf("required %d parameters, but %d parameters are given", i+1, len(args))
			}
			arg := args[i]
			i++
			s, err := conn.convertNamedArgToString(arg)
			if err != nil {
				return "", fmt.Errorf("failed to convert parameter %d: %w", i, err)
			}
			builder.WriteString(s)
		case lexer.KindEOF:
			if isOrdinalArgs && i < len(args) {
				return "", fmt.Errorf("required %d parameters, but %d parameters are given", i, len(args))
			}
			if isNamedArgs {
				for _, arg := range args {
					if !usedByName[":"+arg.Name] {
						return "", fmt.Errorf("named parameter is not used: %s", arg.Name)
					}
				}
			}
			return builder.String(), nil
		default:
			builder.WriteString(token.Value)
		}
	}
	return "", fmt.Errorf("unexpected EOF query: %s", query)
}

func (conn *s3SelectConn) convertNamedArgToString(arg driver.NamedValue) (string, error) {
	switch v := arg.Value.(type) {
	case string:
		return `'` + strings.ReplaceAll(v, "'", "''") + `'`, nil
	case []byte:
		return `'` + strings.ReplaceAll(string(v), "'", "''") + `'`, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case time.Time:
		return `'` + v.Format(time.RFC3339) + `'`, nil
	case nil:
		return "NULL", nil
	default:
		return "", fmt.Errorf("unsupported parameter type: %T", v)
	}
}
