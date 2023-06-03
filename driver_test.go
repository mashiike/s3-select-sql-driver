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
	"time"

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

func TestAWS__SimpleJSONLinesQuery(t *testing.T) {
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
		ObjectKey:  os.Getenv("TEST_OBJECT_PATH_PREFIX") + "json_lines/",
		Format:     S3SelectFormatJSONL,
		ParseTime:  aws.Bool(true),
	}).String()
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		sql := `SELECT s."time", s."message", s."status", s."ttl" FROM S3Object as s WHERE s."time" <= ? AND s.status = ?`
		rows, err := db.QueryContext(context.Background(),
			sql, time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), 200,
		)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		columns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"_1", "_2", "_3", "_4"}, columns)
		actual := make([][]interface{}, 0, 2)
		for rows.Next() {
			var (
				timeVal time.Time
				msg     string
				status  int64
				ttl     float64
			)
			err := rows.Scan(&timeVal, &msg, &status, &ttl)
			require.NoError(t, err)
			actual = append(actual, []interface{}{
				timeVal, msg, status, ttl,
			})
		}
		require.ElementsMatch(t, [][]interface{}{
			{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "Hello World!", int64(200), float64(0.5)},
			{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "Hello Cecily!", int64(200), float64(0.5)},
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

func TestMock__FailedSelectObjectContent(t *testing.T) {
	query := `SELECT * FROM S3Object`
	mockClients["failed_select_object_content"] = &mockS3SelectClient{
		SelectObjectContentWithWriterFunc: func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
			return errors.New("select object content failed")
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		Params:     url.Values{"mock": []string{"failed_select_object_content"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		_, err := db.QueryContext(context.Background(), query)
		require.EqualError(t, err, "select object content failed")
	})
}

func TestMock__FailedListObjectV2__AccessDeneided(t *testing.T) {
	query := `SELECT * FROM S3Object`
	mockClients["failed_list_object_v2_access_denied"] = &mockS3SelectClient{
		ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("AccessDenied: s3:ListObjects")
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/",
		Format:     S3SelectFormatCSV,
		Params:     url.Values{"mock": []string{"failed_list_object_v2_access_denied"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		_, err := db.QueryContext(context.Background(), query)
		require.EqualError(t, err, "AccessDenied: s3:ListObjects")
	})
}

func TestMock__SuccessWithPlaceholder(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = ? AND s."user" = ?`
	mockClients["success_with_placeholder"] = &mockS3SelectClient{
		SelectObjectContentWithWriterFunc: func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
			require.EqualValues(t,
				`SELECT * FROM S3Object as s WHERE s."time" = '2020-01-01T00:00:00Z' AND s."user" = 'hoge'`,
				string(*params.Expression),
			)
			fmt.Fprintf(w, "2020-01-01T00:00:00Z,hoge\n")
			return nil
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"success_with_placeholder"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			d, "hoge",
		)
		var time time.Time
		var user string
		require.NoError(t, row.Scan(&time, &user))
		require.EqualValues(t, d, time)
		require.Equal(t, "hoge", user)
	})
}

func TestMock__FailedWithPlaceholder__MissingParameter(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = ? AND s."user" = ?`
	mockClients["failed_with_placeholder_missing_parameter"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_placeholder_missing_parameter"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			d,
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "required 2 parameters, but 1 parameters are given")
	})
}

func TestMock__FailedWithPlaceholder__MissmatchParameter(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = ? AND s."user" = ?`
	mockClients["failed_with_placeholder_missmatch_parameter"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_placeholder_missmatch_parameter"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			d, "hoge", "fuga",
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "required 2 parameters, but 3 parameters are given")
	})
}

func TestMock__FailedWithPlaceholder__InvalidParameter(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = ? AND s."user" = ?`
	mockClients["failed_with_placeholder_invalid_parameter"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_placeholder_invalid_parameter"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			sql.Named("time", d), sql.Named("user", "hoge"),
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "required ordinal parameter, but named parameter is given")
	})
}

func TestMock__SuccessWithNamedPlaceholder(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = :time AND s."user" = :user`
	mockClients["success_with_named_placeholder"] = &mockS3SelectClient{
		SelectObjectContentWithWriterFunc: func(ctx context.Context, w io.Writer, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) error {
			require.EqualValues(t,
				`SELECT * FROM S3Object as s WHERE s."time" = '2020-01-01T00:00:00Z' AND s."user" = 'hoge'`,
				string(*params.Expression),
			)
			fmt.Fprintf(w, "2020-01-01T00:00:00Z,hoge\n")
			return nil
		},
	}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"success_with_named_placeholder"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			sql.Named("time", d), sql.Named("user", "hoge"),
		)
		var time time.Time
		var user string
		require.NoError(t, row.Scan(&time, &user))
		require.EqualValues(t, d, time)
		require.Equal(t, "hoge", user)
	})
}

func TestMock__FailedWithNamedPlaceholder__MissingParameter(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = :time AND s."user" = :user`
	mockClients["failed_with_named_placeholder_missing_parameter"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_named_placeholder_missing_parameter"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			sql.Named("time", d),
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "missing named parameter: user")
	})
}

func TestMcok__FailedWithNamedPlaceholder__NotUse(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = :time AND s."user" = :user`
	mockClients["failed_with_named_placeholder_not_use"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_named_placeholder_not_use"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			sql.Named("time", d), sql.Named("user", "hoge"), sql.Named("age", 12),
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "named parameter is not used: age")
	})
}

func TestMock__FailedWithNamedPlaceholder__InvalidParameter(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = :time AND s."user" = :user`
	mockClients["failed_with_named_placeholder_invalid_parameter"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_with_named_placeholder_invalid_parameter"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			d, "hoge",
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "required named parameter, but ordinal parameter is given")
	})
}

func TestMock__FailedMixedNamedAndOrdinalPlaceholder(t *testing.T) {
	query := `SELECT * FROM S3Object as s WHERE s."time" = ? AND s."user" = :user`
	mockClients["failed_mixed_named_and_ordinal_placeholder"] = &mockS3SelectClient{}
	mockDSN := (&S3SelectConfig{
		BucketName: "example-com",
		ObjectKey:  "csv/data.csv",
		Format:     S3SelectFormatCSV,
		ParseTime:  aws.Bool(true),
		Params:     url.Values{"mock": []string{"failed_mixed_named_and_ordinal_placeholder"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		row := db.QueryRowContext(
			context.Background(), query,
			d, sql.Named("user", "hoge"),
		)
		var time time.Time
		var user string
		require.EqualError(t, row.Scan(&time, &user), "cannot use both named and ordinal parameters")
	})
}
