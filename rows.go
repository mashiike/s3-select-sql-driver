package s3selectsqldriver

import (
	"database/sql/driver"
	"io"
	"time"
)

type s3SelectRows struct {
	parseTime bool
	columns   []string
	rows      [][]interface{}
	index     int
}

func newRows(columns []string, rows [][]interface{}, parseTime bool) *s3SelectRows {
	return &s3SelectRows{
		parseTime: parseTime,
		columns:   columns,
		rows:      rows,
	}
}

func (rows *s3SelectRows) Close() error {
	return nil
}

func (rows *s3SelectRows) Columns() []string {
	if len(rows.rows) == 0 {
		return []string{}
	}
	return rows.columns
}

func (rows *s3SelectRows) Next(dest []driver.Value) error {
	if rows.index >= len(rows.rows) {
		return io.EOF
	}
	row := rows.rows[rows.index]
	for i := range dest {
		if i >= len(row) {
			dest[i] = nil
			continue
		}
		if str, ok := row[i].(string); ok && rows.parseTime {
			if t, ok := parseTime(str); ok {
				dest[i] = t
				continue
			}
		}
		if i < len(row) {
			dest[i] = row[i]
		}
	}
	rows.index++
	return nil
}

func parseTime(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, true
	}
	return time.Time{}, false
}
