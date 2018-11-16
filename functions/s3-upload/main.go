// +build !test

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/ellery44/data-loader/internal/dataloader"
	"github.com/go-kit/kit/log"
)

// Response Core response object for if processing is successful
type Response struct {
	Success bool `json:"success"`
}

func main() {

	var (
		sess         = session.Must(session.NewSession())
		ssmSvc       = ssm.New(sess)
		s3Svc        = s3.New(sess)
		logger       = dataloader.NewLogger(os.Stdout)
		env          = os.Getenv("ENVIRONMENT_NAME")
		dataBucket   = os.Getenv("DATA_BUCKET")
		schemaBucket = os.Getenv("SCHEMA_BUCKET")
	)

	if env == "" {
		panic("ENVIRONMENT_NAME undefined")
	}
	if dataBucket == "" {
		panic("DATA_BUCKET undefined")
	}
	if schemaBucket == "" {
		panic("SCHEMA_BUCKET undefined")
	}

	// Fetch DB password
	passwordRsp, err := ssmSvc.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String(fmt.Sprintf("/data-loader/%s/master-password", env)),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}
	// Fetch cluster endpoint
	endpointRsp, err := ssmSvc.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String(fmt.Sprintf("/data-loader/%s/cluster-endpoint", env)),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	// Establish DB connection
	db, err := sql.Open("postgres",
		fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
			*endpointRsp.Parameter.Value,
			5439,
			"admin",
			*passwordRsp.Parameter.Value,
			fmt.Sprintf("data-loader-%s", env)),
	)
	if err != nil {
		panic(err)
	}

	// Start up lambda handler
	lambda.Start(func(ctx context.Context, event events.S3Event) (*Response, error) {
		lc, _ := lambdacontext.FromContext(ctx)
		h := handler{
			dl: &dataloader.DataLoader{
				Env:          env,
				DB:           db,
				DataBucket:   dataBucket,
				Logger:       log.With(logger, "request_id", lc.AwsRequestID),
				SchemaBucket: schemaBucket,
				S3Svc:        s3Svc,
			},
		}
		return h.handle(ctx, event)
	})
}
