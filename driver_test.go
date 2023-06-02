package s3selectsqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

var mockClients = map[string]S3SelectClient{}

func init() {
	S3SelectClientConstructor = func(ctx context.Context, cfg *S3SelectConfig) (S3SelectClient, error) {
		if mockName := cfg.Params.Get("mock"); mockName != "" {
			return mockClients[mockName], nil
		}
		return DefaultS3SelectClientConstructor(ctx, cfg)
	}
}

func runTestsWithDB(t *testing.T, dsn string, tests ...func(*testing.T, *sql.DB)) {
	if dsn == "" {
		t.Log("dsn is empty")
		t.SkipNow()
	}
	db, err := sql.Open("s3-select", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	for _, test := range tests {
		test(t, db)
	}
}

func TestAWS__SimpleCSVQuery(t *testing.T) {
	if os.Getenv("TEST_BUCKET_NAME") == "" {
		t.Log("TEST_BUCKET_NAME is empty")
		t.SkipNow()
	}
	if os.Getenv("TEST_OBJECT_PATH_PREFIX") == "" {
		t.Log("TEST_OBJECT_PATH_PREFIX is empty")
		t.SkipNow()
	}
	dsn := (&S3SelectConfig{
		BucketName: os.Getenv("TEST_BUCKET_NAME"),
		ObjectKey:  os.Getenv("TEST_OBJECT_PATH_PREFIX") + "csv/",
		Format:     S3SelectFormatCSV,
	}).String()
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		sql := `SELECT * FROM S3Object`
		rows, err := db.QueryContext(context.Background(), sql)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		columns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"_1", "_2"}, columns)
		actual := make([][]interface{}, 0, 2)
		for rows.Next() {
			var (
				num  int64
				text string
			)
			err := rows.Scan(&num, &text)
			require.NoError(t, err)
			actual = append(actual, []interface{}{
				num, text,
			})
		}
		require.ElementsMatch(t, [][]interface{}{
			{int64(1), "hoge"},
			{int64(2), "fuga"},
			{int64(3), "piyo"},
			{int64(4), "tora"},
		}, actual)
	})
}

func TestMock__Success(t *testing.T) {
	query := `SELECT * FROM S3Object`
	mockClients["success"] = &mockS3SelectClient{
		ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "example-com", *params.Bucket)
			require.Equal(t, "csv/", *params.Prefix)
			// expected 2 calls, 1st call return 1 object, 2nd call return 0 object
			if params.ContinuationToken == nil {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{
						{Key: aws.String("csv/1.csv")},
						{Key: aws.String("csv/2.csv")},
					},
					Name:                  aws.String("example-com"),
					Prefix:                aws.String("csv/"),
					NextContinuationToken: aws.String("dummy"),
				}, nil
			}
			return &s3.ListObjectsV2Output{
				Contents: []types.Object{},
				KeyCount: 1,
				MaxKeys:  1000,
				Name:     aws.String("example-com"),
				Prefix:   aws.String("csv/"),
			}, nil
		},
		SelectObjectContentWithWriterFunc: func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
			require.Equal(t, "example-com", *params.Bucket)
			if *params.Key == "csv/1.csv" {
				fmt.Fprintf(w, "hoge,16,true,1.1,123\n")
				fmt.Fprintf(w, "fuga,23,false,2.2,456\n")
				return nil
			}
			if *params.Key == "csv/2.csv" {
				fmt.Fprintf(w, "piyo,12,true,3.3,789\n")
				fmt.Fprintf(w, "tora,23,false,4.4,012\n")
				return nil
			}
			return errors.New("unexpected key")
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/",
		Format:     S3SelectFormatCSV,
		Params:     url.Values{"mock": []string{"success"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err)
		actualColumns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"_1", "_2", "_3", "_4", "_5"}, actualColumns)
		actual := make([]map[string]interface{}, 0, 4)
		for rows.Next() {
			var (
				name  string
				age   int64
				auth  sql.NullBool
				value sql.NullFloat64
				bin   []byte
			)
			err := rows.Scan(&name, &age, &auth, &value, &bin)
			require.NoError(t, err)
			actual = append(actual, map[string]interface{}{
				"name":  name,
				"age":   age,
				"auth":  auth,
				"value": value,
				"bin":   bin,
			})
			t.Log(name, age, auth, value, bin)
		}
		require.Equal(t, []map[string]interface{}{
			{
				"name": "hoge",
				"age":  int64(16),
				"auth": sql.NullBool{
					Bool:  true,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 1.1,
					Valid:   true,
				},
				"bin": []byte("123"),
			},
			{
				"name": "fuga",
				"age":  int64(23),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 2.2,
					Valid:   true,
				},
				"bin": []byte("456"),
			},
			{
				"name": "piyo",
				"age":  int64(12),
				"auth": sql.NullBool{
					Bool:  true,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 3.3,
					Valid:   true,
				},
				"bin": []byte("789"),
			},
			{
				"name": "tora",
				"age":  int64(23),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 4.4,
					Valid:   true,
				},
				"bin": []byte("012"),
			},
		}, actual)
	})
}

func TestMock__SuccessSingleObject(t *testing.T) {
	query := `SELECT * FROM S3Object`
	mockClients["success_single_object"] = &mockS3SelectClient{
		SelectObjectContentWithWriterFunc: func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
			fmt.Fprintf(w, "hoge,16,true,1.1,123\n")
			fmt.Fprintf(w, "fuga,23,false,2.2,456\n")
			fmt.Fprintf(w, "piyo,12,true,3.3,789\n")
			fmt.Fprintf(w, "tora,23,false,4.4,012\n")
			return nil
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		Params:     url.Values{"mock": []string{"success_single_object"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err)
		actualColumns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"_1", "_2", "_3", "_4", "_5"}, actualColumns)
		actual := make([]map[string]interface{}, 0, 4)
		for rows.Next() {
			var (
				name  string
				age   int64
				auth  sql.NullBool
				value sql.NullFloat64
				bin   []byte
			)
			err := rows.Scan(&name, &age, &auth, &value, &bin)
			require.NoError(t, err)
			actual = append(actual, map[string]interface{}{
				"name":  name,
				"age":   age,
				"auth":  auth,
				"value": value,
				"bin":   bin,
			})
		}
		require.Equal(t, []map[string]interface{}{
			{
				"name": "hoge",
				"age":  int64(16),
				"auth": sql.NullBool{
					Bool:  true,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 1.1,
					Valid:   true,
				},
				"bin": []byte("123"),
			},
			{
				"name": "fuga",
				"age":  int64(23),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 2.2,
					Valid:   true,
				},
				"bin": []byte("456"),
			},
			{
				"name": "piyo",
				"age":  int64(12),
				"auth": sql.NullBool{
					Bool:  true,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 3.3,
					Valid:   true,
				},
				"bin": []byte("789"),
			},
			{
				"name": "tora",
				"age":  int64(23),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 4.4,
					Valid:   true,
				},
				"bin": []byte("012"),
			},
		}, actual)
	})
}
