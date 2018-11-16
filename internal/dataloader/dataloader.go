package dataloader

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type dBColumnSchema struct {
	Name     string
	Width    string
	DataType string
}

// DataLoader takes care of core functionality around data load for this application.
type DataLoader struct {
	DB           *sql.DB
	Logger       log.Logger
	Env          string
	DataBucket   string
	SchemaBucket string
	S3Svc        s3iface.S3API
}

// LoadDataFileToRedshift takes a filename as input and loads it into designated target bucket
func (d *DataLoader) LoadDataFileToRedshift(ctx context.Context, fileName string) error {

	targetName := strings.Split(fileName, "_")[0]

	rawSchema, err := d.fetchTableSchema(ctx, targetName)
	if err != nil {
		return err
	}

	schema, err := marshalTableSchema(rawSchema)
	if err != nil {
		return err
	}

	redShiftTableExists, err := d.checkIfRedShiftTableExists(ctx, targetName)
	if err != nil {
		return err
	}

	if !redShiftTableExists {
		err = d.createTable(ctx, targetName, schema)
		if err != nil {
			return err
		}
	}

	return d.executeRedShiftCopyCommand(ctx, schema, targetName, fileName)
}

// Red Shift Actions  ------------------------

// Checks if passed table exists in target redshift cluster
func (d *DataLoader) checkIfRedShiftTableExists(ctx context.Context, tableName string) (bool, error) {
	const tableExistsQuery = `SELECT TRUE WHERE EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = $1);`
	rows, err := d.DB.QueryContext(ctx, tableExistsQuery, tableName)
	if err != nil {
		return false, err
	}
	if !rows.Next() {
		return false, nil
	}
	return true, nil
}

// Executes a redshift COPY command from passed to copyTarget s3 file into passed targetFile
func (d *DataLoader) executeRedShiftCopyCommand(ctx context.Context, schema []dBColumnSchema, tableName, copyTarget string) error {
	start := time.Now()
	level.Info(d.Logger).Log("msg", "attempting copy command",
		"table_name", tableName,
		"copy_target", copyTarget)
	_, err := d.DB.ExecContext(ctx, d.buildCopyFromS3Query(schema, tableName, copyTarget))
	if err != nil {
		level.Error(d.Logger).Log("msg", "copy command failure",
			"elapsed_time", time.Now().Sub(start),
			"table_name", tableName,
			"copy_target", copyTarget,
			"err", err)
		return err
	}
	level.Info(d.Logger).Log("msg", "copy command complete",
		"elapsed_time", time.Now().Sub(start),
		"table_name", tableName,
		"copy_target", copyTarget)
	return err
}

// Creates table in target redshift DB with passed expectedSchema
func (d *DataLoader) createTable(ctx context.Context, tableName string, schema []dBColumnSchema) error {
	level.Info(d.Logger).Log("msg", "table not found creating new one", "table_name", tableName)
	createTableQuery, err := d.buildCreateTableQuery(tableName, schema)
	if err != nil {
		return err
	}
	_, err = d.DB.ExecContext(ctx, createTableQuery)
	if err != nil {
		level.Info(d.Logger).Log("msg", "created table successfully", "table_name", tableName)
	}
	return err
}

// S3 Actions ----------------------------

// Fetches the expectedSchema CSV from remote s3 bucket.
func (d *DataLoader) fetchTableSchema(ctx context.Context, targetName string) (io.ReadCloser, error) {
	level.Info(d.Logger).Log("msg", "loading data expectedSchema", "schema_bucket", d.SchemaBucket, "schema_name", targetName)
	schemaRsp, err := d.S3Svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.SchemaBucket),
		Key:    aws.String(targetName + ".csv"),
	})
	if err != nil {
		return nil, err
	}
	return schemaRsp.Body, nil
}

// Query Builders ----------------------

// Builds CREATE TABLE query from passed table name and expectedSchema. Try's to make TEXT fields as small as possible
// based off width.
func (d *DataLoader) buildCreateTableQuery(tableName string, dbColumns []dBColumnSchema) (string, error) {

	// Validate col length
	numOfCol := len(dbColumns)
	if numOfCol == 0 || numOfCol > 1600 {
		// https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_usage.html
		return "", fmt.Errorf("invalid number of columns defined in expectedSchema: %d", numOfCol)
	}

	//Loop thorough columns build up query
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s(", tableName))
	prefix := ""
	set := make(map[string]struct{})
	for _, colProps := range dbColumns {

		// Check for duplicate column name
		if _, ok := set[colProps.Name]; ok {
			return "", fmt.Errorf("duplicate column name passed in expectedSchema: %s", colProps.Name)
		}
		set[colProps.Name] = struct{}{}

		sb.WriteString(prefix)
		renderedDataType := ""
		switch colProps.DataType {
		case "TEXT":

			// Check for too large of width for text field
			textWidth, err := strconv.Atoi(colProps.Width)
			if err != nil {
				return "", err
			}
			if !(textWidth <= 256) {
				// https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html
				return "", fmt.Errorf("passed column width %s is larger then TEXT field allows", colProps.Width)
			}

			// Create column with minimum bytes needed based off expectedSchema width
			renderedDataType = fmt.Sprintf("VARCHAR(%s)", colProps.Width)

		case "INTEGER":
			fallthrough
		case "BOOLEAN":
			renderedDataType = colProps.DataType
		default:
			return "", fmt.Errorf("unknown data type passed %s", colProps.DataType)
		}
		sb.WriteString(fmt.Sprintf(" %s %s", colProps.Name, renderedDataType))
		prefix = ","
	}
	sb.WriteString(");")

	generatedQuery := sb.String()
	level.Debug(d.Logger).Log("msg", "built create table query", "generated_query", generatedQuery)
	return generatedQuery, nil
}

// Builds a COPY query from passed target details. Leverages fixed width data format based off passed expectedSchema.
func (d *DataLoader) buildCopyFromS3Query(schema []dBColumnSchema, tableName, copyTarget string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"COPY %s FROM 's3://%s/%s' IAM_ROLE 'arn:aws:iam::%s:role/%s' FIXEDWIDTH '",
		tableName,
		d.DataBucket,
		copyTarget,
		"653026974230",
		fmt.Sprintf("data-loader-redshift-copy-%s-us-west-2", d.Env)))

	// Add copy cmd 'fixedwidth' config based off passed expectedSchema
	prefix := ""
	for _, colProps := range schema {
		sb.WriteString(prefix)
		sb.WriteString(fmt.Sprintf("%s:%s", colProps.Name, colProps.Width))
		prefix = ", "
	}
	sb.WriteString("';")

	generatedQuery := sb.String()
	level.Debug(d.Logger).Log("msg", "built copy from query", "generated_query", generatedQuery)
	return generatedQuery
}

// Marshaller's  ------------------------

// Converts expectedSchema from CSV to struct we can use to build create table query
func marshalTableSchema(rawSchema io.ReadCloser) ([]dBColumnSchema, error) {
	lines, err := csv.NewReader(rawSchema).ReadAll()
	if err != nil {
		return nil, err
	}
	// Loop through lines & turn into struct
	var dbColumns []dBColumnSchema
	for i, line := range lines {
		// Skip column def line of csv
		if i == 0 {
			continue
		}
		dbColumns = append(dbColumns, dBColumnSchema{
			Name:     line[0],
			Width:    line[1],
			DataType: line[2],
		})
	}
	return dbColumns, nil
}

// Utility Functions ------------------------

// NewLogger builds a standard logger object that follows best practices.
func NewLogger(w io.Writer) log.Logger {
	logger := log.With(
		log.NewLogfmtLogger(log.NewSyncWriter(w)),
		"ts", log.DefaultTimestampUTC,
	)
	if debug() {
		logger = level.NewFilter(logger, level.AllowAll())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	stdlog.SetFlags(0)
	stdlog.SetOutput(log.NewStdlibAdapter(logger))
	return logger
}

func debug() bool {
	if os.Getenv("AWS_SAM_LOCAL") == "true" {
		return true
	}
	if os.Getenv("DEBUG") != "" {
		return true
	}
	if os.Getenv("debug") != "" {
		return true
	}
	return false
}
