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

import (
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
			break
		}
		log.Printf("%s\t%s", timestamp, message)
	}
}
```

### Placeholders

S3 Select SQL driver supports placeholders.

for example, can use ordinal placeholders

```go
rows, err := db.QueryContext(
    context.Background(),
    `SELECT timestamp, message FROM s3object s WHERE timestamp > ?`,
    time.Now().Add(-time.Hour),
)
```

and named placeholders

```go
rows, err := db.QueryContext(
    context.Background(),
    `SELECT timestamp, message FROM s3object s WHERE timestamp > :timestamp`,
    sql.Named("timestamp", time.Now().Add(-time.Hour)),
)
```

### DSN format

```
s3://<bucket>/<key>?<query>
```

#### query parameters

|name|description|default|
|---|---|---|
|format|object format (csv,tsv,json,json_lines,parquet)|file ext auto detect |
|compression_type|gzip or bzip, none|none|
|parse_time|parse time column|false|
|input_serialization|input serialization base64 json|<nil>|
|region|aws region|<nil>|

#### input serialization base64 json 

if set complex format, you can set input serialization in DSN

see also S3 Select [InputSerialization](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html#RESTObjectSELECTContent-InputSerialization)

```json
{
  "CSV": {
    "FileHeaderInfo": "NONE",
    "RecordDelimiter": "\n",
    "FieldDelimiter": ",",
    "QuoteCharacter": "\"",
    "QuoteEscapeCharacter": "\"",
    "Comments": "#",
    "AllowQuotedRecordDelimiter": false
  },
  "CompressionType": "NONE"
}
```

and can set DSN query parameter(json base64 encoded)

```
s3://example-com/hoge.csv?input_serialization=ewogICJDU1YiOiB7CiAgICAiRmlsZUhlYWRlckluZm8iOiAiTk9ORSIsCiAgICAiUmVjb3JkRGVsaW1pdGVyIjogIlxuIiwKICAgICJGaWVsZERlbGltaXRlciI6ICIsIiwKICAgICJRdW90ZUNoYXJhY3RlciI6ICJcIiIsCiAgICAiUXVvdGVFc2NhcGVDaGFyYWN0ZXIiOiAiXCIiLAogICAgIkNvbW1lbnRzIjogIiMiLAogICAgIkFsbG93UXVvdGVkUmVjb3JkRGVsaW1pdGVyIjogZmFsc2UKICB9LAogICJDb21wcmVzc2lvblR5cGUiOiAiTk9ORSIKfQo
```

### Prefix Search and LIMIT clause 

S3 Select SQL driver supports prefix search and LIMIT clause.
for example:
DSN is `s3://example-com/data/?format=csv&compression_type=gzip`
Query is `SELECT * FROM s3object s LIMIT 10`

S3 Select SQL driver search `s3://example-com/data/` prefix and get 10 objects.
case of following objects in bucket.

```
s3://example-com/data/2020-01-01.csv.gz
s3://example-com/data/2020-01-02.csv.gz
s3://example-com/data/2020-01-03.csv.gz
s3://example-com/data/2020-01-04.csv.gz
...
```

each object is 5 rows, execute S3 Select object is `2020-01-01.csv.gz` and `2020-01-02.csv.gz` and get 10 rows.

## LICENSE

MIT


