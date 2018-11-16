package dataloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"io/ioutil"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-kit/kit/log"
)

func TestBuildCreateTableQuery(t *testing.T) {
	tests := []struct {
		name   string
		svc    *DataLoader
		schema []dBColumnSchema
		err    error
		want   string
	}{
		{
			name: "happy-path",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
				{Width: "42", Name: "testCol2", DataType: "TEXT"},
				{Width: "8", Name: "testCol3", DataType: "BOOLEAN"},
				{Width: "4", Name: "testCol4", DataType: "INTEGER"},
			},
			err:  nil,
			want: "CREATE TABLE testTable( testCol1 VARCHAR(4), testCol2 VARCHAR(42), testCol3 BOOLEAN, testCol4 INTEGER);",
		},
		{
			name: "expectedSchema-empty",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{},
			err:    errors.New("invalid number of columns defined in expectedSchema: 0"),
		},
		{
			name: "expectedSchema-to-many-columns",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: buildHugeSchema(),
			err:    errors.New("invalid number of columns defined in expectedSchema: 1601"),
		},
		{
			name: "expectedSchema-unsupported-type",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol", DataType: "BAD_TYPE"},
			},
			err: errors.New("unknown data type passed BAD_TYPE"),
		},
		{
			name: "expectedSchema-text-width-to-large",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "257", Name: "testCol", DataType: "TEXT"},
			},
			err: errors.New("passed column width 257 is larger then TEXT field allows"),
		},
		{
			name: "expectedSchema-duplicate-column",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol", DataType: "TEXT"},
				{Width: "4", Name: "testCol", DataType: "TEXT"},
			},
			err: errors.New("duplicate column name passed in expectedSchema: testCol"),
		},
		{
			name: "expectedSchema-bad-width",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "a", Name: "testCol", DataType: "TEXT"},
			},
			err: errors.New("strconv.Atoi: parsing \"a\": invalid syntax"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.svc.buildCreateTableQuery("testTable", tt.schema)
			if tt.err != nil && tt.err.Error() != err.Error() {
				t.Errorf("want: %v, got: %v", tt.err, err)
			}
			if tt.want != got {
				t.Errorf("want: %s, got: %s", tt.want, got)
			}
		})
	}
}

func TestCreateCopyCommandQuery(t *testing.T) {
	tests := []struct {
		name   string
		svc    *DataLoader
		schema []dBColumnSchema
		want   string
	}{
		{
			name: "happy-path-1",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
				{Width: "42", Name: "testCol2", DataType: "TEXT"},
				{Width: "8", Name: "testCol3", DataType: "BOOLEAN"},
				{Width: "4", Name: "testCol4", DataType: "INTEGER"},
			},
			want: "COPY testtable FROM 's3://testDB/testtarget' " +
				"IAM_ROLE 'arn:aws:iam::653026974230:role/data-loader-redshift-copy--us-west-2' " +
				"FIXEDWIDTH 'testCol1:4, testCol2:42, testCol3:8, testCol4:4';",
		},
		{
			name: "happy-path-2",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
			},
			want: "COPY testtable FROM 's3://testDB/testtarget' " +
				"IAM_ROLE 'arn:aws:iam::653026974230:role/data-loader-redshift-copy--us-west-2' " +
				"FIXEDWIDTH 'testCol1:4';",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.svc.buildCopyFromS3Query(tt.schema, "testtable", "testtarget")
			if tt.want != got {
				t.Errorf("want: %s, got: %s", tt.want, got)
			}
		})
	}
}

func TestCreateTable(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Errorf("An error '%s' was not expected when opening a stub database connection", err)
	}
	tests := []struct {
		name   string
		svc    *DataLoader
		schema []dBColumnSchema
		err    error
		want   string
	}{
		{
			name: "happy-path",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				DB:         db,
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
				{Width: "42", Name: "testCol2", DataType: "TEXT"},
				{Width: "8", Name: "testCol3", DataType: "BOOLEAN"},
				{Width: "4", Name: "testCol4", DataType: "INTEGER"},
			},
			want: "CREATE TABLE testtable( testCol1 VARCHAR(4), testCol2 VARCHAR(42), testCol3 BOOLEAN, testCol4 INTEGER);",
		},
		{
			name: "bad-query-build",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				DB:         db,
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
			},
			err: fmt.Errorf("duplicate column name passed in expectedSchema: testCol1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.svc.createTable(context.Background(), "testtable", tt.schema)
			if tt.err != nil && tt.err.Error() != err.Error() {
				t.Errorf("want: %v, got: %v", tt.err, err)
			}
			mockRsp := err.Error()
			if !strings.Contains(mockRsp, tt.want) {
				t.Errorf("want: %s, got: %s", tt.want, mockRsp)
			}

		})
	}
}

func TestExecuteCopy(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Errorf("An error '%s' was not expected when opening a stub database connection", err)
	}
	tests := []struct {
		name   string
		svc    *DataLoader
		schema []dBColumnSchema
		err    error
		want   string
	}{
		{
			name: "happy-path-1",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				DB:         db,
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol1", DataType: "TEXT"},
				{Width: "42", Name: "testCol2", DataType: "TEXT"},
				{Width: "8", Name: "testCol3", DataType: "BOOLEAN"},
				{Width: "4", Name: "testCol4", DataType: "INTEGER"},
			},
			want: "COPY testtable FROM 's3://testDB/testtarget' " +
				"IAM_ROLE 'arn:aws:iam::653026974230:role/data-loader-redshift-copy--us-west-2' " +
				"FIXEDWIDTH 'testCol1:4, testCol2:42, testCol3:8, testCol4:4';",
		},
		{
			name: "happy-path-2",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				DB:         db,
			},
			schema: []dBColumnSchema{
				{Width: "4", Name: "testCol4", DataType: "INTEGER"},
			},
			want: "COPY testtable FROM 's3://testDB/testtarget' " +
				"IAM_ROLE 'arn:aws:iam::653026974230:role/data-loader-redshift-copy--us-west-2' " +
				"FIXEDWIDTH 'testCol4:4';",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.svc.executeRedShiftCopyCommand(context.Background(), tt.schema, "testtable", "testtarget")
			if tt.err != nil && tt.err.Error() != err.Error() {
				t.Errorf("want: %v, got: %v", tt.err, err)
			}
			mockRsp := err.Error()
			if !strings.Contains(mockRsp, tt.want) {
				t.Errorf("want: %s, got: %s", tt.want, mockRsp)
			}

		})
	}
}

func TestFetchTableSchema(t *testing.T) {
	tests := []struct {
		name string
		svc  *DataLoader
		err  error
	}{
		{
			name: "happy-path",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				S3Svc: &mockS3{
					errToReturn: nil,
				},
			},
			err: nil,
		},
		{
			name: "s3-read-error",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
				S3Svc: &mockS3{
					errToReturn: errors.New("test_error"),
				},
			},
			err: errors.New("test_error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.svc.fetchTableSchema(context.Background(), "foo")
			if tt.err != nil && tt.err.Error() != err.Error() {
				t.Errorf("want: %v, got: %v", tt.err, err)
			}
		})
	}
}



func TestMarshalSchema(t *testing.T) {
	tests := []struct {
		name           string
		svc            *DataLoader
		expectedSchema []dBColumnSchema
		err            error
		rawSchema      string
	}{
		{
			name: "happy-path",
			svc: &DataLoader{
				DataBucket: "testDB",
				Logger:     log.NewNopLogger(),
			},
			expectedSchema: []dBColumnSchema{
				{Width: "10", Name: "name", DataType: "TEXT"},
				{Width: "1", Name: "valid", DataType: "BOOLEAN"},
				{Width: "3", Name: "count", DataType: "INTEGER"},
			},
			err: nil,
			rawSchema: `
"column name",width,datatype
name,10,TEXT
valid,1,BOOLEAN
count,3,INTEGER
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshalTableSchema(ioutil.NopCloser(bytes.NewReader([]byte(tt.rawSchema))))
			if tt.err != nil && tt.err.Error() != err.Error() {
				t.Errorf("want: %v, got: %v", tt.err, err)
			}
			for i, dBColumnSchema := range tt.expectedSchema {
				if got[i].Name != dBColumnSchema.Name ||
					got[i].Width != dBColumnSchema.Width ||
					got[i].DataType != dBColumnSchema.DataType {

						t.Errorf("want: %+v, got: %+v", tt.expectedSchema, got)
				}
			}
		})
	}
}

func TestLog(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(buf)
	logger.Log("msg", "Hello World")
	msgLogged := buf.String()
	r, _ := regexp.Compile("ts=+.* msg=\\\"Hello World\\\"")
	if !r.MatchString(msgLogged) {
		t.Errorf("message logged did not meet regex: %s", msgLogged)
	}
}

// Mock Services -------------

type mockS3 struct {
	s3iface.S3API
	errToReturn error
}

func (c *mockS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if c.errToReturn != nil {
		return nil, c.errToReturn
	}
	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader([]byte("foo"))),
	}, nil
}

// Helper functions --------

func buildHugeSchema() []dBColumnSchema {
	returnSchema := make([]dBColumnSchema, 1601)
	for i := range [1601]int{} {
		returnSchema[i] = dBColumnSchema{DataType: "INTEGER", Name: "test", Width: "1"}
	}
	return returnSchema
}
