package s3selectsqldriver

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestS3SelectConfig__String(t *testing.T) {
	cases := []struct {
		dsn      *S3SelectConfig
		expected string
	}{
		{
			dsn: &S3SelectConfig{
				BucketName: "example-com",
				ObjectKey:  "csv/data.csv",
			},
			expected: "s3://example-com/csv/data.csv",
		},
		{
			dsn: (&S3SelectConfig{
				BucketName: "example-com",
				ObjectKey:  "csv/data.csv",
			}).WithRegion("us-east-1"),
			expected: "s3://example-com/csv/data.csv?region=us-east-1",
		},
		{
			dsn: (&S3SelectConfig{
				BucketName: "example-com",
				ObjectKey:  "csv/data.csv",
			}).WithRegion("us-east-1"),
			expected: "s3://example-com/csv/data.csv?region=us-east-1",
		},
		{
			dsn: (&S3SelectConfig{
				BucketName:      "example-com",
				ObjectKeyPrefix: "csv/",
			}).WithRegion("us-east-1"),
			expected: "s3://example-com/csv/?region=us-east-1",
		},
	}

	for _, c := range cases {
		t.Run(c.expected, func(t *testing.T) {
			actual := c.dsn.String()
			require.Equal(t, c.expected, actual)
		})
	}
}

func TestS3SelectConfig__ParseDSN(t *testing.T) {
	cases := []struct {
		dsn      string
		expected *S3SelectConfig
	}{
		{
			dsn: "s3://example-com/csv/data.csv",
			expected: &S3SelectConfig{
				BucketName:      "example-com",
				ObjectKey:       "csv/data.csv",
				CompressionType: S3SelectCompressionTypeNone,
				Format:          S3SelectFormatCSV,
			},
		},
		{
			dsn: "s3://example-com/csv/data.parquet?region=us-east-1",
			expected: (&S3SelectConfig{
				BucketName:      "example-com",
				ObjectKey:       "csv/data.parquet",
				CompressionType: S3SelectCompressionTypeNone,
				Format:          S3SelectFormatParquet,
			}).WithRegion("us-east-1"),
		},
		{
			dsn: "s3://example-com/csv/data.gz?format=json_lines&region=us-east-1&compression_type=none",
			expected: (&S3SelectConfig{
				BucketName:      "example-com",
				ObjectKey:       "csv/data.gz",
				CompressionType: S3SelectCompressionTypeNone,
				Format:          S3SelectFormatJSONL,
			}).WithRegion("us-east-1"),
		},
		{
			dsn: "s3://example-com/csv/data.gz?region=us-east-1&input_serialization=" + base64.URLEncoding.EncodeToString([]byte(`{
				"CompressionType": "GZIP",
				"CSV": {
					"FileHeaderInfo": "NONE",
					"RecordDelimiter": "\n",
					"FieldDelimiter": ",",
					"QuoteCharacter": "\"",
					"QuoteEscapeCharacter": "\"",
					"Comments": "#"
				}
			}`)),
			expected: (&S3SelectConfig{
				BucketName: "example-com",
				ObjectKey:  "csv/data.gz",
				InputSerialization: &types.InputSerialization{
					CompressionType: types.CompressionTypeGzip,
					CSV: &types.CSVInput{
						FileHeaderInfo:       types.FileHeaderInfoNone,
						RecordDelimiter:      aws.String("\n"),
						FieldDelimiter:       aws.String(","),
						QuoteCharacter:       aws.String("\""),
						QuoteEscapeCharacter: aws.String("\""),
						Comments:             aws.String("#"),
					},
				},
			}).WithRegion("us-east-1"),
		},
		{
			dsn: "s3://example-com/csv/?format=json_lines&region=us-east-1&compression_type=none",
			expected: (&S3SelectConfig{
				BucketName:      "example-com",
				ObjectKeyPrefix: "csv/",
				CompressionType: S3SelectCompressionTypeNone,
				Format:          S3SelectFormatJSONL,
			}).WithRegion("us-east-1"),
		},
	}

	for _, c := range cases {
		t.Run(c.dsn, func(t *testing.T) {
			actual, err := ParseDSN(c.dsn)
			require.NoError(t, err)
			require.Equal(t, len(c.expected.S3OptFns), len(actual.S3OptFns))
			actual.S3OptFns = c.expected.S3OptFns
			require.EqualValues(t, c.expected, actual)
		})
	}

}
