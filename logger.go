package s3selectsqldriver

import (
	"errors"
	"io"
	"log"
	"os"
)

type Logger interface {
	Printf(format string, v ...any)
	SetOutput(w io.Writer)
	Writer() io.Writer
}

var errLogger = Logger(log.New(os.Stderr, "[s3-select][error]", log.Ldate|log.Ltime|log.Lshortfile))
var debugLogger = Logger(log.New(io.Discard, "[s3-select][debug]", log.Ldate|log.Ltime|log.Lshortfile))

func SetLogger(l Logger) error {
	if l == nil {
		return errors.New("logger is nil")
	}
	errLogger = l
	return nil
}

func SetDebugLogger(l Logger) error {
	if l == nil {
		return errors.New("logger is nil")
	}
	debugLogger = l
	return nil
}
