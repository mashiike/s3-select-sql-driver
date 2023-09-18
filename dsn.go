package s3selectsqldriver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3SelectFormat string

const (
	S3SelectFormatCSV     S3SelectFormat = "csv"
	S3SelectFormatTSV     S3SelectFormat = "tsv"
	S3SelectFormatJSON    S3SelectFormat = "json"
	S3SelectFormatParquet S3SelectFormat = "parquet"
	S3SelectFormatJSONL   S3SelectFormat = "json_lines"
)

type S3SelectCompressionType string

const (
	S3SelectCompressionTypeNone  S3SelectCompressionType = "none"
	S3SelectCompressionTypeGzip  S3SelectCompressionType = "gzip"
	S3SelectCompressionTypeBzip2 S3SelectCompressionType = "bzip2"
)

type S3SelectConfig struct {
	BucketName         string
	ObjectKey          string
	ObjectKeyPrefix    string
	Format             S3SelectFormat
	CompressionType    S3SelectCompressionType
	InputSerialization *types.InputSerialization
	ParseTime          *bool
	Params             url.Values
	S3OptFns           []func(*s3.Options)
}

func (cfg *S3SelectConfig) String() string {
	var path string
	if cfg.ObjectKey != "" {
		path = "/" + cfg.ObjectKey
	}
	if cfg.ObjectKeyPrefix != "" {
		path = "/" + cfg.ObjectKeyPrefix
	}
	u := &url.URL{
		Scheme: "s3",
		Host:   cfg.BucketName,
		Path:   path,
	}
	params := url.Values{}
	for key, value := range cfg.Params {
		params[key] = append([]string{}, value...)
	}
	if cfg.Format != "" {
		params.Add("format", string(cfg.Format))
	} else {
		params.Del("format")
	}
	if cfg.CompressionType != "" {
		params.Add("compression_type", string(cfg.CompressionType))
	} else {
		params.Del("compression_type")
	}
	if cfg.ParseTime != nil {
		params.Add("parse_time", strconv.FormatBool(*cfg.ParseTime))
	} else {
		params.Del("parse_time")
	}
	if cfg.InputSerialization != nil {
		bs, err := json.Marshal(cfg.InputSerialization)
		if err == nil {
			params.Add("input_serialization", base64.URLEncoding.EncodeToString(bs))
		}
	} else {
		params.Del("input_serialization")
	}
	u.RawQuery = params.Encode()
	return u.String()
}

func (cfg *S3SelectConfig) setParams(params url.Values) error {
	cfg.Params = params
	if params.Has("region") {
		cfg = cfg.WithRegion(params.Get("region"))
	}
	var formatSet bool
	if params.Has("format") {
		switch strings.ToLower(params.Get("format")) {
		case "csv":
			cfg.Format = S3SelectFormatCSV
		case "tsv":
			cfg.Format = S3SelectFormatTSV
		case "json":
			cfg.Format = S3SelectFormatJSON
		case "parquet":
			cfg.Format = S3SelectFormatParquet
		case "jsonl", "jsonlines", "json_lines":
			cfg.Format = S3SelectFormatJSONL
		default:
			return fmt.Errorf("unknown format: %s", params.Get("format"))
		}
		cfg.Params.Del("format")
		formatSet = true
	}
	var comporessionTypeSet bool
	if params.Has("compression_type") {
		switch strings.ToLower(params.Get("compression_type")) {
		case "none":
			cfg.CompressionType = S3SelectCompressionTypeNone
		case "gzip":
			cfg.CompressionType = S3SelectCompressionTypeGzip
		case "bzip2":
			cfg.CompressionType = S3SelectCompressionTypeBzip2
		default:
			return fmt.Errorf("unknown compression_type: %s", params.Get("compression_type"))
		}
		cfg.Params.Del("compression_type")
		comporessionTypeSet = true
	} else {
		cfg.CompressionType = S3SelectCompressionTypeNone
	}
	if params.Has("parse_time") {
		parseTime, err := strconv.ParseBool(params.Get("parse_time"))
		if err != nil {
			return fmt.Errorf("parse parse_time: %w", err)
		}
		cfg.ParseTime = &parseTime
		cfg.Params.Del("parse_time")
	}
	var inputSerializationSet bool
	if params.Has("input_serialization") {
		if formatSet {
			return errors.New("format and input_serialization are exclusive")
		}
		var inputSerialization types.InputSerialization
		bs, err := base64.URLEncoding.DecodeString(params.Get("input_serialization"))
		if err != nil {
			return fmt.Errorf("decode input_serialization: %w", err)
		}
		if err := json.Unmarshal(bs, &inputSerialization); err != nil {
			return fmt.Errorf("unmarshal input_serialization: %w", err)
		}
		cfg.InputSerialization = &inputSerialization
		cfg.Params.Del("input_serialization")
		inputSerializationSet = true
		cfg.CompressionType = ""
		cfg.Format = ""
	}
	if len(cfg.Params) == 0 {
		cfg.Params = nil
	}
	if !formatSet && !inputSerializationSet {
		var detected bool
		remain := cfg.ObjectKey
		for ext := filepath.Ext(remain); ext != ""; ext = filepath.Ext(remain) {
			remain = strings.TrimSuffix(remain, ext)
			switch ext {
			case ".csv":
				cfg.Format = S3SelectFormatCSV
				detected = true
			case ".tsv":
				cfg.Format = S3SelectFormatTSV
				detected = true
			case ".json":
				cfg.Format = S3SelectFormatJSON
				detected = true
			case ".parquet":
				cfg.Format = S3SelectFormatParquet
				detected = true
			case ".jsonl", ".jsonlines":
				cfg.Format = S3SelectFormatJSONL
				detected = true
			case ".gz":
				if !comporessionTypeSet {
					cfg.CompressionType = S3SelectCompressionTypeGzip
				}
			case ".bz2":
				if !comporessionTypeSet {
					cfg.CompressionType = S3SelectCompressionTypeBzip2
				}
			}
		}
		if !detected {
			return errors.New("format is not set")
		}
	}
	return nil
}

func ParseDSN(dsn string) (*S3SelectConfig, error) {
	if dsn == "" {
		return nil, ErrDSNEmpty
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, errors.New("dsn scheme not s3")
	}
	if u.Host == "" {
		return nil, errors.New("dsn bucket name is empty")
	}
	if u.Path == "" {
		return nil, errors.New("dsn object key is empty")
	}
	cfg := &S3SelectConfig{
		BucketName: u.Host,
	}
	if strings.HasSuffix(u.Path, filepath.Base(u.Path)) {
		cfg.ObjectKey = strings.TrimPrefix(u.Path, "/")
	} else {
		cfg.ObjectKeyPrefix = strings.TrimPrefix(u.Path, "/")
	}
	if err := cfg.setParams(u.Query()); err != nil {
		return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
	}
	return cfg, nil
}

func (cfg *S3SelectConfig) WithRegion(region string) *S3SelectConfig {
	if cfg.Params == nil {
		cfg.Params = url.Values{}
	}
	cfg.Params.Set("region", region)
	cfg.S3OptFns = append(cfg.S3OptFns, func(o *s3.Options) {
		o.Region = region
	})
	return cfg
}

func (cfg *S3SelectConfig) newInputSeliarization() (*types.InputSerialization, error) {
	if cfg.InputSerialization != nil {
		return cfg.InputSerialization, nil
	}
	var ret *types.InputSerialization
	switch cfg.Format {
	case S3SelectFormatCSV:
		ret = &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo:  types.FileHeaderInfoUse,
				FieldDelimiter:  aws.String(","),
				RecordDelimiter: aws.String("\n"),
			},
		}
	case S3SelectFormatTSV:
		ret = &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo:  types.FileHeaderInfoUse,
				FieldDelimiter:  aws.String("\t"),
				RecordDelimiter: aws.String("\n"),
			},
		}
	case S3SelectFormatJSON:
		ret = &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: types.JSONTypeDocument,
			},
		}
	case S3SelectFormatParquet:
		ret = &types.InputSerialization{
			Parquet: &types.ParquetInput{},
		}
	case S3SelectFormatJSONL:
		ret = &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: types.JSONTypeLines,
			},
		}
	default:
		return nil, errors.New("unknown format")
	}
	switch cfg.CompressionType {
	case S3SelectCompressionTypeNone:
		ret.CompressionType = types.CompressionTypeNone
	case S3SelectCompressionTypeBzip2:
		ret.CompressionType = types.CompressionTypeBzip2
	case S3SelectCompressionTypeGzip:
		ret.CompressionType = types.CompressionTypeGzip
	default:
		return nil, errors.New("unknown compression type")
	}
	return ret, nil
}
