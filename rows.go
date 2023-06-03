package s3selectsqldriver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/samber/lo"
)

type s3SelectRows struct {
	parseTime bool
	rows      [][]string
	index     int
}

func newRows(rows [][]string, parseTime bool) *s3SelectRows {
	return &s3SelectRows{
		parseTime: parseTime,
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
	return lo.Map(rows.rows[0], func(_ string, i int) string {
		return fmt.Sprintf("_%d", i+1)
	})
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
		if rows.parseTime {
			if t, ok := parseTime(row[i]); ok {
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
