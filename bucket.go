package main

import (
	"fmt"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

func processBucket(endpoint, bucketName, accessKey, secretKey string) <- chan *s3.ListResp {
	cli := s3.New(aws.Auth{AccessKey: accessKey, SecretKey: secretKey},
				  aws.Region{Name: "generic", S3Endpoint: endpoint})
	bucket := cli.Bucket(bucketName)
	responses := make(chan *s3.ListResp)
	go func() {
		prefix := ""
		delim  := ""
		marker := ""
		limit  := 1000
		hasMore := true

		defer close(responses)
		for hasMore {

			result, err := bucket.List(prefix, delim, marker, limit)
			if err != nil {
				fmt.Println("Some trouble", err.Error())
				return
			}
			responses <- result
			hasMore = result.IsTruncated
			marker = result.NextMarker
			fmt.Printf("hasMore %v marker %v, len %v\n\n", hasMore, marker, len(result.Contents))
		}
	}()
	return responses
}
