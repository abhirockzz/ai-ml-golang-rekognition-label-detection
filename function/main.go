package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
)

var rekognitionClient *rekognition.Client
var dynamodbClient *dynamodb.Client
var table string

func init() {
	table = os.Getenv("TABLE_NAME")

	if table == "" {
		log.Fatal("missing environment variable TABLE_NAME")
	}

	cfg, err := config.LoadDefaultConfig(context.Background())

	if err != nil {
		log.Fatal("failed to load config ", err)
	}

	rekognitionClient = rekognition.NewFromConfig(cfg)
	dynamodbClient = dynamodb.NewFromConfig(cfg)

}

func handler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {

		fmt.Println("file", record.S3.Object.Key, "uploaded to", record.S3.Bucket.Name)

		sourceBucketName := record.S3.Bucket.Name
		fileName := record.S3.Object.Key

		err := labelDetection(sourceBucketName, fileName)

		if err != nil {
			log.Fatal("failed to process file ", record.S3.Object.Key, " in bucket ", record.S3.Bucket.Name, err)
		}
	}
}

func main() {
	lambda.Start(handler)
}

func labelDetection(sourceBucketName, fileName string) error {

	resp, err := rekognitionClient.DetectLabels(context.Background(), &rekognition.DetectLabelsInput{
		Image: &types.Image{
			S3Object: &types.S3Object{
				Bucket: aws.String(sourceBucketName),
				Name:   aws.String(fileName),
			},
		},
	})
	if err != nil {
		return err
	}

	fmt.Println("labels detected in file", fileName, "from bucket", sourceBucketName)

	for _, label := range resp.Labels {
		item := make(map[string]ddbTypes.AttributeValue)

		item["source_file"] = &ddbTypes.AttributeValueMemberS{Value: fileName}

		fmt.Println("label name", aws.ToString(label.Name))
		item["label_name"] = &ddbTypes.AttributeValueMemberS{Value: *label.Name}

		for _, c := range label.Categories {
			fmt.Println("category", *c.Name)
		}
		item["label_category"] = &ddbTypes.AttributeValueMemberS{Value: *label.Categories[0].Name}

		fmt.Println("confidence", aws.ToFloat32(label.Confidence))

		item["label_confidence"] = &ddbTypes.AttributeValueMemberN{Value: fmt.Sprintf("%v", aws.ToFloat32(label.Confidence))}

		fmt.Println("==============")

		_, err := dynamodbClient.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item:      item,
		})

		if err != nil {
			return err
		}

		fmt.Println("label added to table")
	}

	return nil
}
