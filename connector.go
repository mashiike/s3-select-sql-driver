package s3selectsqldriver

import (
	"context"
	"database/sql/driver"
)

type s3SelectConnector struct {
	d   *s3SelectDriver
	cfg *S3SelectConfig
}

func (c *s3SelectConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := newS3SelectClient(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return newConn(client, c.cfg), nil
}

func (c *s3SelectConnector) Driver() driver.Driver {
	return c.d
}
