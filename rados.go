package main

import (
	radosAPI "github.com/mjarco/go-radosgw/pkg/api"
	"log"
	"sync"
)

const USERS_WORKERS_COUNT = 5

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
	bucketCredsChan := make(chan bucketCreds)
	userNames := make(chan string)

	wg := sync.WaitGroup{}
	wg.Add(USERS_WORKERS_COUNT)

	for workerNo := 1; workerNo <= USERS_WORKERS_COUNT; workerNo++ {
		go func(workerNo int) {
			usersCredsCount := 0
			for userName := range userNames {
				utasks, err := processUser(api, userName, ac.Endpoint)
				if err == nil {
					for _, cred := range utasks {
						bucketCredsChan <- cred
						usersCredsCount++
					}
				} else {
					log.Printf("ERROR from 'processUser': %v (in worker: %v)\n", err, workerNo)
				}
			}
			log.Printf("STATS processUser - workerNo: %v, usersCredsCount: %v\n", workerNo, usersCredsCount)
			wg.Done()
		}(workerNo)
	}

	go func() {
		for bucketCredsItem := range bucketCredsChan {
			bucketCredsList = append(bucketCredsList, bucketCredsItem)
		}
	}()

	for _, userName := range allUsers {
		userNames <- userName
	}

	close(userNames)
	wg.Wait()
	close(bucketCredsChan)

	accessKeysCounts := make(map[string]int)

	for _, bc := range bucketCredsList {
		accessKeysCounts[bc.accessKey] += 1
	}
	log.Printf("STATS listBucketsWithAuth - accessKeysCounts: %v, len(accessKeysCounts): %v, len(allUsers): %v\n",
		accessKeysCounts, len(accessKeysCounts), len(allUsers))

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
