# s3-select-sql-driver

[![Documentation](https://godoc.org/github.com/mashiike/s3-select-sql-driver?status.svg)](https://godoc.org/github.com/mashiike/s3-select-sql-driver)
![Latest GitHub tag](https://img.shields.io/github/tag/mashiike/s3-select-sql-driver.svg)
![Github Actions test](https://github.com/mashiike/s3-select-sql-driver/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/s3-select-sql-driver)](https://goreportcard.com/report/mashiike/s3-select-sql-driver)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/s3-select-sql-driver/blob/master/LICENSE)

S3 Select sql driver for Go's [database/sql](https://pkg.go.dev/database/sql) package

# Usage 

for example:

```go 
package main

import(
	"context"
	"database/sql"
	"log"
    "time"

	_ "github.com/mashiike/s3-select-sql-driver"
)

func main() {
	db, err := sql.Open("s3-select", "s3://example-com/abc.csv?format=csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(
		context.Background(),
		`SELECT timestamp, message FROM s3object s`,
	)
	if err != nil {
		log.Fatalln(err)
	}
    defer rows.Close()
	for rows.Next() {
        var timestamp time.Time
		var message string
		err := rows.Scan(&timestamp, &message)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("%s\t%s", teimstamp, message)
	}
}
```

## LICENSE

MIT


