package moresql

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// workerCount dedicated workers per collection
	workerCount = 5

	// workerCountOverflow Threads in Golang 1.6+ are ~4kb to start
	// 500 * 4k = ~2MB ram usage due to heap of each routine
	workerCountOverflow = 500

	// reportFrequency the timing for how often to report activity
	reportFrequency = 60 // seconds

	// checkpointFrequency frequency at which checkpointing is saved to DB
	checkpointFrequency = time.Duration(30) * time.Second

	// type of tail log
	optLog       = "optlog"
	changeStream = "change-stream"

	// export to
	postgresExport = "postgres"
	csvExport      = "csv"
	mongoExport    = "mongo"
)

var wg sync.WaitGroup

func Run() {
	c := Commands{}
	env := FetchEnvsAndFlags()
	SetupLogger(env)
	ExitUnlessValidEnv(env)

	log.WithFields(log.Fields{"params": fmt.Sprintf("%+v", env)}).Info("Environment")
	config := LoadConfig(env.configFile)
	var pg *sqlx.DB
	if len(env.urls.postgres) > 0 {
		pg = GetPostgresConnection(env)
		log.Info("Connected to postgres")
		defer pg.Close()
	}

	// TODO: should this run for each execution of application
	if env.validatePostgres {
		c.ValidateTablesAndColumns(config, pg)
	}

	var client *mongo.Client
	if len(env.urls.mongo) > 0 {
		client = GetMongoConnection(env.urls.mongo)
		defer client.Disconnect(context.TODO())
		log.Info("Connected to mongo")
	}


	var clientExport *mongo.Client
	if len(env.urls.mongoExport) > 0 {
		clientExport = GetMongoConnection(env.urls.mongoExport)
		log.Info("Connected to mongo export")
		defer clientExport.Disconnect(context.TODO())
	}

	if env.monitor {
		go http.ListenAndServe(":1234", nil)
	}

	switch {
	case env.sync:
		FullSync(config, pg, client, clientExport)
	case env.tail:
		Tail(config, pg, env, client , clientExport)
	case env.syncFile:
		SyncFile(config, pg, env)
	default:
		flag.Usage()
	}
}
