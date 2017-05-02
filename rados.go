package main

import (
	radosAPI "github.com/mjarco/go-radosgw/pkg/api"
	"log"
)

type bucketCreds struct {
	endpoint, bucketName, accessKey, secretKey string
}

type AdminConf struct {
	Endpoint, AdminAccessKey, AdminSecretKey, AdminPrefix string
}

func listBucketsWithAuth(ac AdminConf) ([]bucketCreds, error) {
	api, err := radosAPI.New(ac.Endpoint, ac.AdminAccessKey, ac.AdminSecretKey, ac.AdminPrefix)
	if err != nil {
		return nil, err
	}
	allUsers, err := api.GetUsers()
	if err != nil {
		return nil, err
	}
	bucketCredsList := []bucketCreds{}
	for _, userName := range allUsers {
		utasks, err := processUser(api, userName, ac.Endpoint)
		if err != nil {
			return bucketCredsList, err
		}
		bucketCredsList = append(bucketCredsList, utasks...)
	}
	return bucketCredsList, nil
}

func processUser(api *radosAPI.API, userName, endpoint string) ([]bucketCreds, error) {
	log.Println("Processing user ", userName)
	user, err := api.GetUser(userName)
	if err != nil {
		return nil, err
	}

	userBuckets, err := api.GetBucket(radosAPI.BucketConfig{UID: userName})
	if err != nil {
		return nil, err
	}

	tasks := []bucketCreds{}
	for _, bucket := range userBuckets {
		if bucket.Name == "" {
			continue
		}
		tasks = append(tasks, bucketCreds{
			bucketName: bucket.Name,
			accessKey:  user.Keys[0].AccessKey,
			secretKey:  user.Keys[0].SecretKey,
			endpoint:   endpoint,
		})
	}
	return tasks, nil
}
