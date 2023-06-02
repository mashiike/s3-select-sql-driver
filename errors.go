package s3selectsqldriver

import "errors"

var (
	ErrNotSupported = errors.New("not supported")
	ErrDSNEmpty     = errors.New("dsn is empty")
)
