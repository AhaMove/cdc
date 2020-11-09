package moresql

import (
	"bufio"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"os"
)

func SyncFile(config Config, pg *sqlx.DB, env Env) {
	validateSyncFileParams(env)

	file, err := os.Open(env.syncFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		processInLine(scanner.Text(), env.syncFileCollection, env.syncFileDatabase, config, pg)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func validateSyncFileParams(env Env) {
	if len(env.syncFilePath) == 0 {
		log.Warnf(`Missing required variable. SYNC_FILE_PATH must be set.`)
		os.Exit(1)
	}

	if len(env.syncFileCollection) == 0 {
		log.Warnf(`Missing required variable. SYNC_FILE_COLLECTION must be set.`)
		os.Exit(1)
	}

	if len(env.syncFileDatabase) == 0 {
		log.Warnf(`Missing required variable. SYNC_FILE_DATABASE must be set.`)
		os.Exit(1)
	}
}

func processInLine(in, collectionName, db string, config Config, pg *sqlx.DB) {
	st := FullSyncer{Config: config}
	o, c := st.statementFromDbCollection(db, collectionName)
	data, err := SanitizeDataFile(c, in, true)
	if err != nil {
		log.WithFields(log.Fields{"collection": collectionName, "error": err, "data": in}).Fatal("Error SanitizeData")
	}
	pg.NamedExec(o.BuildUpsert(), data)
}
