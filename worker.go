package main

import (
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/allegro/akubra/log"
)

type s3MigrationAuth struct {
	endpoint  string
	accessKey string
	secretKey string
	region    string
}

type s3MigrationObj struct {
	bucket      string
	object      string
	contentType string
	perm        s3.ACL
	options     s3.Options
	metadata    map[string]string
}

func getS3Client(s3Auth s3MigrationAuth) *s3.S3 {
	cli := s3.New(aws.Auth{AccessKey: s3Auth.accessKey, SecretKey: s3Auth.secretKey},
		aws.Region{Name: "generic", S3Endpoint: s3Auth.endpoint})
	log.Debug("cli: %v", cli)
	return cli
}

func prepareS3Clients(srcS3Auth s3MigrationAuth, dstS3Auth s3MigrationAuth) (*s3.S3, *s3.S3) {
	return getS3Client(srcS3Auth), getS3Client(dstS3Auth)
}

func CopyS3Object(migrationObj s3MigrationObj, srcS3Client s3.S3, dstS3Client s3.S3) (bool, error) {
	log.Debug("CopyS3Object START")

	log.Debug("migrationObj.bucket: %v", migrationObj.bucket)

	srcBucket := srcS3Client.Bucket(migrationObj.bucket)

	log.Debug("srcBucket: %v", srcBucket)

	srcData, err := srcBucket.Get(migrationObj.object)
	if err != nil {
		log.Debug("ERR SRC BUCKET GET: %v", err)
		return false, err
	}

	log.Debug("srcData: %s", len(string(srcData)))

	dstBucket := dstS3Client.Bucket(migrationObj.bucket)
	err = dstBucket.PutBucket(migrationObj.perm)
	if err != nil {
		log.Debug("ERR DST PutBucket: %v", err)
		return false, err
	}

	err = dstBucket.Put(migrationObj.object, srcData, migrationObj.contentType, migrationObj.perm, migrationObj.options)
	if err != nil {
		log.Debug("ERR DST PUT OBJECT: %v", err)
		return false, err
	}

	log.Debug("CopyS3Object END")
	return true, nil
}
