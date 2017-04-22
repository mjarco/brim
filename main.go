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

type clasifiedKey struct {
	path, clusterName string
}

func configure () (RadosConf, error) {
	rc := RadosConf{}
	confFile, err := os.Open(*brimConf)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with opening config file: '%s' - err: %v !", *brimConf, err)
		return rc, err
	}
	defer confFile.Close()

	bs, err := ioutil.ReadAll(confFile)
	if err != nil {
		return rc, err
	}
	err = yaml.Unmarshal(bs, &rc)
	if err != nil {
		return rc, err
	}
	fmt.Printf("%v\n", rc)
	return rc, err
}

func clasifier(
	ring sharding.ShardsRing,
	listResp *s3.ListResp,
	clasified chan<-clasifiedKey) {
	for _, key := range listResp.Contents {
		path := listResp.Name + "/" + key.Key
		cl, err := ring.Pick(path)
		if err != nil {
			fmt.Println(err.Error())
		}
		clasified <- clasifiedKey{path, cl.Name}
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

func main() {
	versionString := fmt.Sprintf("Akubra (%s version)", version)
	kingpin.Version(versionString)
	kingpin.Parse()
	ring, err := mkRing()
	if err != nil {
		log.Fatalf("Cannot find ring: %s", err.Error())
	}
	rc, err := configure()
	if err != nil {
		log.Fatalf("Cannot read brim config")
	}
	print("list buckets")
	bucketsCreds, err := listBucketsWithAuth(rc)
	print("got buckets")
	if err != nil {
		log.Fatalf("Problems with fetching buckets list with keys", err.Error())
	}

	for _, bc := range bucketsCreds {
		fmt.Printf("\n%v\n\n", bc)
		bucketListing := processBucket(bc.endpoint, bc.bucketName,
			bc.accessKey, bc.secretKey)
		if bucketListing == nil {
			continue // ?
		}
		for listResp := range bucketListing {
			cks := make (chan clasifiedKey)
			go func(){
				for clasifiedKey := range cks {
					store(clasifiedKey)
				}
			}()
			clasifier(ring, listResp, cks)
			close(cks)
		}

	}


}
