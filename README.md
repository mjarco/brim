# Brim #

## Goal ##
Brim migrates objects between [Akubra](http://github.com/allegro/akubra) shards 
in case of changes to sharding policies, and synchronizes backends if inconsistency
is detected.

## Requirements

* postgresql

setup table 

    CREATE TABLE migration (
        "from" character varying(255),
        "to" character varying(255),
        key character varying(1024),
        env character varying(255),
        pid integer,
        mid bigint NOT NULL,
        date timestamp without time zone DEFAULT now(),
        last_update timestamp without time zone DEFAULT now(),
        status character varying(255)
    );


create brim.yaml 

    # admin endpoint
    endpoint: "http://ceph-admin.internal.service"
    adminprefix: "admin"
    adminaccesskey: "*****"
    adminsecretkey: "*********"
    # s3 endpoint
    bucketendpoint: "http://s3.internal.service"
    database:
      user: postgres
      password: ""
      dbname: brim
      host: localhost
      inserttmpl: |
        INSERT INTO migration("from", "to", key, env, pid, status)
        VALUES ('{{.From}}', '{{.To}}', '{{.Key}}', '{{.Env}}', 0, 'new');

# run

    go build .
    ./brim -c ./real-akubra.conf.yaml -b brim.yaml

