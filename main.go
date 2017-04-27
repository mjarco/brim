package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"github.com/allegro/akubra/sharding"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/log"
	"net/http"

	// shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/alecthomas/kingpin"
	yaml "gopkg.in/yaml.v2"
	"github.com/AdRoll/goamz/s3"
	)

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
	path, sourceCluster, targetCluster string
}

type BrimConf struct {
	Database DBConfig
	Admins map[string][]AdminConf
	urlToCluster map[string]string
}

func (bc *BrimConf) endpointClusterMap () map[string]string {
	if bc.urlToCluster == nil {
		bc.urlToCluster = make(map[string]string)
		for key, endpoints := range(bc.Admins) {
			for _, adminConf := range(endpoints) {
				bc.urlToCluster[adminConf.Endpoint] = key
			}
		}
	}
	return bc.urlToCluster
}

func configure () (BrimConf, error) {
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
	classified chan<-classifiedKey,
	source string,
	endpointClusterMap map[string]string) {
	for _, key := range listResp.Contents {
		path := listResp.Name + "/" + key.Key
		cl, err := ring.Pick(path)
		if err != nil {
			fmt.Println(err.Error())
		}
		dest := cl.Name
		target := endpointClusterMap[source]

		if source == target {
			continue
		}

		classified <- classifiedKey{
			path,
			source,
			target,
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
	bucketsCreds, err := listBucketsWithAuth(ac)
	if err != nil {
		log.Fatalf("Problems with fetching buckets list with keys", err.Error())
	}

	for _, bc := range bucketsCreds {
		bucketListing := processBucket(bc.endpoint, bc.bucketName,
			bc.accessKey, bc.secretKey)
		if bucketListing == nil {
			continue // ?
		}
		for listResp := range bucketListing {
			cks := make (chan classifiedKey)
			go func(){
				for classifiedKey := range cks {
					storage.store(classifiedKey)
				}
			}()
			classifier(ring, listResp, cks, source, endpointClusterMap)
			close(cks)
		}
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
	processCluster(bc.Admins["prod"][0], storage, ring, bc.endpointClusterMap())
}
