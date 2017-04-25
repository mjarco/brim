package main

import (
	radosAPI "github.com/mjarco/go-radosgw/pkg/api"
)

type bucketCreds struct {
	endpoint, bucketName, accessKey, secretKey string
}

type RadosConf struct {
	Endpoint, AdminAccessKey, AdminSecretKey, AdminPrefix, BucketEndpoint string
	Database DBConfig
}

func listBucketsWithAuth(rc RadosConf) ([]bucketCreds, error) {
	api, err := radosAPI.New(rc.Endpoint, rc.AdminAccessKey, rc.AdminSecretKey, rc.AdminPrefix)
	if err != nil {
		return nil, err
	}
	allUsers, err :=  api.GetUsers()
	if err != nil {
		return nil, err
	}
	tasks := []bucketCreds{}
	for _, userName := range allUsers {
		utasks, err := processUser(api, userName, rc.BucketEndpoint)
		if err != nil {
			return tasks, err
		}
		tasks = append(tasks, utasks...)
	}
	return tasks, nil
}


func processUser(api *radosAPI.API, userName, endpoint string) ([]bucketCreds, error) {
	println("Processing user ", userName)
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
			accessKey: user.Keys[0].AccessKey,
			secretKey: user.Keys[0].SecretKey,
			endpoint: endpoint,
		})
	}
	return tasks, nil
}