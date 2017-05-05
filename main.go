package main

import (
	"fmt"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/sharding"
	"io/ioutil"
	"net/http"
	"os"

	// shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/AdRoll/goamz/s3"
	"github.com/alecthomas/kingpin"
	yaml "gopkg.in/yaml.v2"
)

const BUCKET_WORKERS_COUNT = 5

var (
	// filled by linker
	version = "development"

	// CLI flags
	configFile = kingpin.
			Flag("config", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('c').
			Required().
			ExistingFile()
	brimConf = kingpin.
			Flag("brim-config", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('b').
			Required().
			ExistingFile()
)

type classifiedKey struct {
	path, sourceRegion, targetRegion string
}

type BrimConf struct {
	Database    DBConfig
	Admins      map[string][]AdminConf
	urlToRegion map[string]string
}

func (bc *BrimConf) endpointRegionMapping() map[string]string {
	if bc.urlToRegion == nil {
		bc.urlToRegion = make(map[string]string)
		for key, endpoints := range bc.Admins {
			for _, adminConf := range endpoints {
				bc.urlToRegion[adminConf.Endpoint] = key
			}
		}
	}
	return bc.urlToRegion
}

func configure() (BrimConf, error) {
	bc := BrimConf{}
	confFile, err := os.Open(*brimConf)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with opening config file: '%s' - err: %v !", *brimConf, err)
		return bc, err
	}
	defer confFile.Close()

	bs, err := ioutil.ReadAll(confFile)
	if err != nil {
		return bc, err
	}
	err = yaml.Unmarshal(bs, &bc)
	if err != nil {
		return bc, err
	}
	fmt.Printf("%v\n", bc)
	return bc, err
}

func classifier(
	ring sharding.ShardsRing,
	listResp *s3.ListResp,
	classified chan<- classifiedKey,
	source string,
	endpointRegionMap map[string]string) {

	// keys listing from cluster bucket
	for _, key := range listResp.Contents {
		path := listResp.Name + "/" + key.Key
		region, err := ring.Pick(path)
		if err != nil {
			fmt.Println(err.Error())
		}
		dest := region.Name
		src := endpointRegionMap[source]

		if src == dest {
			continue
		}

		classified <- classifiedKey{
			path,
			src,
			dest,
		}
	}
}

func mkRing() (sharding.ShardsRing, error) {
	conf, err := config.Configure(*configFile)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}
	rings := sharding.NewRingFactory(conf, &http.Transport{})
	ring, err := rings.ClientRing(*conf.Client)
	return ring, err
}

func processCluster(
	ac AdminConf,
	storage *dbStorage,
	ring sharding.ShardsRing,
	endpointClusterMap map[string]string) {

	source := ac.Endpoint
	bucketsCredsList, err := listBucketsWithAuth(ac)
	if err != nil {
		log.Fatalf("Problems with fetching buckets list with keys", err.Error())
	}

	buckets := make(chan bucketCreds)

	for workerNo := 1; workerNo <= BUCKET_WORKERS_COUNT; workerNo++ {
		go func(workerNo int, buckets <-chan bucketCreds) {
			for bc := range buckets {
				bucketListing := processBucket(bc.endpoint, bc.bucketName,
					bc.accessKey, bc.secretKey)
				if bucketListing == nil {
					continue // ?
				}
				for listResp := range bucketListing {
					cks := make(chan classifiedKey)
					go func() {
						counter := 0
						for classifiedKey := range cks {
							counter++
							storage.store(classifiedKey)
						}
						log.Printf("STATS CLASSIFIER WORKER: [%d] procced %d items from bucket '%s'.\n",
							workerNo, counter, bc.bucketName)
					}()
					classifier(ring, listResp, cks, source, endpointClusterMap)
					close(cks)
				}
			}
		}(workerNo, buckets)
	}

	for _, bc := range bucketsCredsList {
		buckets <- bc
	}
}

func main() {
	versionString := fmt.Sprintf("Akubra (%s version)", version)
	kingpin.Version(versionString)
	kingpin.Parse()
	ring, err := mkRing()
	if err != nil {
		log.Fatalf("Cannot find ring: %s", err.Error())
	}
	bc, err := configure()
	if err != nil {
		log.Fatalf("Cannot read brim config %s", err.Error())
	}

	storage := &dbStorage{
		config: bc.Database,
	}
	processCluster(bc.Admins["prod"][0], storage, ring, bc.endpointRegionMapping())
}
