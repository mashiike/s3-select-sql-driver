package s3selectsqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("s3-select", &s3SelectDriver{})
}

type s3SelectDriver struct{}

func (d *s3SelectDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *s3SelectDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &s3SelectConnector{
		d:   d,
		cfg: cfg,
	}, nil
}
