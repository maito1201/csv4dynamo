package dynamo2csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/urfave/cli/v2"
	"os"
	"sort"
	"strconv"
)

var db *dynamodb.DynamoDB

type ScanOption struct {
	TableName            string
	FilterExpression     string
	ExpressionAttrNames  string
	ExpressionAttrValues string
}

func Export(c *cli.Context) error {
	ctx := c.Context

	db = dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           c.String("profile"),
	})))

	option := ScanOption{
		TableName:            c.String("table-name"),
		FilterExpression:     c.String("filter-expression"),
		ExpressionAttrNames:  c.String("expression-attribute-names"),
		ExpressionAttrValues: c.String("expression-attribute-values"),
	}

	w := csv.NewWriter(os.Stdout)
	if c.String("output") != "" {
		f, err := os.OpenFile(c.String("output"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		w = csv.NewWriter(f)
	} else {
		defer w.Flush()
	}

	var startKey map[string]*dynamodb.AttributeValue
	lineCount := 0
	var keyOrder []string
	for {
		resp, sk, err := Scan(ctx, option, startKey)
		if err != nil {
			return fmt.Errorf("scan with key: %w", err)
		}

		for _, attr := range resp {
			if lineCount == 0 {
				for k, _ := range attr {
					keyOrder = append(keyOrder, k)
				}
				sort.Strings(keyOrder)
				_ = w.Write(keyOrder)
			}

			record := make([]string, 0, len(keyOrder))
			for _, k := range keyOrder {
				original := attr[k]
				switch val := original.(type) {
				case float64:
					// protect exponential notation layout
					record = append(record, strconv.FormatFloat(val, 'f', -1, 64))
				case string:
					record = append(record, val)
				default:
					record = append(record, fmt.Sprint(original))
				}
			}
			_ = w.Write(record)
			lineCount++
		}

		startKey = sk
		if len(startKey) == 0 {
			break
		}
	}
	return nil
}

func Scan(ctx context.Context, opt ScanOption, startKey map[string]*dynamodb.AttributeValue) ([]map[string]interface{}, map[string]*dynamodb.AttributeValue, error) {

	var expressionAttributeValues map[string]*dynamodb.AttributeValue
	if opt.ExpressionAttrValues != "" {
		if err := json.Unmarshal([]byte(opt.ExpressionAttrValues), &expressionAttributeValues); err != nil {
			return nil, nil, fmt.Errorf("expression attribute values is invalid: %w", err)
		}
	}

	var expressionAttributeNames map[string]*string
	if opt.ExpressionAttrNames != "" {
		expressionAttributeNames = make(map[string]*string)
		if err := json.Unmarshal([]byte(opt.ExpressionAttrNames), &expressionAttributeNames); err != nil {
			return nil, nil, fmt.Errorf("%w", err)
		}
	}

	var filterExpression *string
	if opt.FilterExpression != "" {
		filterExpression = aws.String(opt.FilterExpression)
	}

	out, err := db.ScanWithContext(ctx, &dynamodb.ScanInput{
		TableName:                 aws.String(opt.TableName),
		ExclusiveStartKey:         startKey,
		ExpressionAttributeNames:  expressionAttributeNames,
		FilterExpression:          filterExpression,
		ExpressionAttributeValues: expressionAttributeValues,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("db.ScanWithContext: %w", err)
	}

	var resp []map[string]interface{}
	if err := dynamodbattribute.UnmarshalListOfMaps(out.Items, &resp); err != nil {
		return nil, nil, fmt.Errorf("dynamodb unmarshal list of maps: %w", err)
	}

	return resp, out.LastEvaluatedKey, nil
}
