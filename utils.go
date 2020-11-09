package moresql

import (
	"encoding/json"
	"flag"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/rwynn/gtm"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func InitByFlag(e *Env) {
	flag.StringVar(&e.configFile, "config-file", "", "Configuration file to use")
	flag.BoolVar(&e.sync, "full-sync", false, "Run full sync for each db.collection in config")
	flag.BoolVar(&e.allowDeletes, "allow-deletes", true, "Allow deletes to propagate from Mongo -> PG")

	flag.BoolVar(&e.syncFile, "sync-file", false, "Get data from file and upsert into pg")
	flag.StringVar(&e.syncFilePath, "sync-file-path", "", "File json location")
	flag.StringVar(&e.syncFileCollection, "sync-file-collection", "", "Specific collection in config")
	flag.StringVar(&e.syncFileDatabase, "sync-file-database", "", "Specific database in config")

	flag.BoolVar(&e.tail, "tail", false, "Tail mongodb for each db.collection in config")
	flag.StringVar(&e.tailType, "tail-type", "optlog", "Select tail type: optlog, change-stream")

	flag.StringVar(&e.SSLCert, "ssl-cert", "", "SSL PEM cert for Mongodb")
	flag.StringVar(&e.appName, "app-name", "moresql", "AppName used in Checkpoint table")
	flag.BoolVar(&e.monitor, "enable-monitor", false, "Run expvarmon endpoint")
	flag.BoolVar(&e.checkpoint, "checkpoint", false, "Store and restore from checkpoints in PG table: moresql_metadata")
	flag.BoolVar(&e.createTableSQL, "create-table-sql", false, "Print out the necessary SQL for creating metadata table required for checkpointing")
	flag.BoolVar(&e.validatePostgres, "validate", false, "Validate the postgres table structures and exit")
	flag.StringVar(&e.errorReporting, "error-reporting", "", "Error reporting tool to use (currently only supporting Rollbar)")
	flag.StringVar(&e.memprofile, "memprofile", "", "Profile memory usage. Supply filename for output of memory usage")

	flag.StringVar(&e.csvPathFile, "csv-path-file", "", "Path to save file, default: /tmp/ahamove.csv")
	flag.StringVar(&e.exports, "exports", "postgres", "Exporting to: postgres, csv or both postgres,csv")

	defaultDuration := time.Duration(0 * time.Second)
	flag.DurationVar(&e.replayDuration, "replay-duration", defaultDuration, "Last x to replay ie '1s', '5m', etc as parsed by Time.ParseDuration. Will be subtracted from time.Now()")
	flag.Int64Var(&e.replaySecond, "replay-second", 0, "Replay a specific epoch second of the oplog and forward from there.")
	flag.BoolVar(&e.SSLInsecureSkipVerify, "ssl-insecure-skip-verify", false, "Skip verification of Mongo SSL certificate ala sslAllowInvalidCertificates")
	flag.IntVar(&e.postgresMaxOpenConns, "postgres-max-open-connections", 20, "Max opening connection in postgres")
	flag.BoolVar(&e.justInsert, "just-insert", false, "Actions db collected: update, delete will be update, delete in db export, respective. If just-insert set true, others actions become insert")
	flag.Parse()
}

func InitByEnv(e *Env) {
	e.urls.mongo = os.Getenv("MONGO_URL")
	e.urls.postgres = os.Getenv("POSTGRES_URL")
	e.urls.mongoExport = os.Getenv("MONGO_EXPORT_URL")

	if len(os.Getenv("REPORTING_TOKEN")) > 0 {
		e.reportingToken = os.Getenv("REPORTING_TOKEN")
	}

	if len(os.Getenv("APP_ENV")) > 0 {
		e.appEnvironment = os.Getenv("APP_ENV")
	}
	if len(os.Getenv("CONFIG_FILE")) > 0 {
		e.configFile = os.Getenv("CONFIG_FILE")
	}
	if sync, err := strconv.ParseBool(os.Getenv("SYNC")); err == nil && sync {
		e.sync = sync
	}

	if tail, err := strconv.ParseBool(os.Getenv("TAIL")); err == nil && tail {
		e.tail = tail
	}

	if len(os.Getenv("TAIL_TYPE")) > 0 {
		e.tailType = os.Getenv("TAIL_TYPE")
	}

	if len(os.Getenv("SSL_CERT")) > 0 {
		e.SSLCert = os.Getenv("SSL_CERT")
	}

	if len(os.Getenv("APP_NAME")) > 0 {
		e.appName = os.Getenv("APP_NAME")
	}

	if monitor, err := strconv.ParseBool(os.Getenv("MONITOR")); err == nil && monitor {
		e.monitor = monitor
	}

	if checkpoint, err := strconv.ParseBool(os.Getenv("CHECK_POINT")); err == nil && checkpoint {
		e.checkpoint = checkpoint
	}

	if createTableSQL, err := strconv.ParseBool(os.Getenv("CREATE_TABLE_SQL")); err == nil && createTableSQL {
		e.createTableSQL = createTableSQL
	}

	if validatePostgres, err := strconv.ParseBool(os.Getenv("VALIDATE_POSTGRES")); err == nil && validatePostgres {
		e.validatePostgres = validatePostgres
	}

	if len(os.Getenv("ERROR_REPORTING")) > 0 {
		e.errorReporting = os.Getenv("ERROR_REPORTING")

	}

	if len(os.Getenv("MEMORY_PROFILE")) > 0 {
		e.memprofile = os.Getenv("MEMORY_PROFILE")
	}
	if len(os.Getenv("CSV_PATH_FILE")) > 0 {
		e.csvPathFile = os.Getenv("CSV_PATH_FILE")
	}

	if len(os.Getenv("EXPORTS")) > 0 {
		e.exports = os.Getenv("EXPORTS")
	}

	if replayDuration, err := strconv.Atoi(os.Getenv("REPLAY_DURATION")); err == nil && replayDuration > 0 {
		e.replayDuration = time.Duration(replayDuration) * time.Second
	}

	if replaySecond, err := strconv.ParseInt(os.Getenv("REPLAY_SECOND"), 10, 64); err == nil && replaySecond > 0 {
		e.replaySecond = replaySecond
	}

	if SSLInsecureSkipVerify, err := strconv.ParseBool(os.Getenv("SSL_INSECURE_SKIP_VERIFY")); err == nil && SSLInsecureSkipVerify {
		e.SSLInsecureSkipVerify = SSLInsecureSkipVerify
	}

	if postgresMaxOpenConns, err := strconv.Atoi(os.Getenv("POSTGRES_MAX_OPERATIONS")); err == nil && postgresMaxOpenConns != 20 {
		e.postgresMaxOpenConns = postgresMaxOpenConns
	}

	if justInsert, err := strconv.ParseBool(os.Getenv("JUST_INSERT")); err == nil && justInsert {
		e.justInsert = justInsert
	}
}

func FetchEnvsAndFlags() (e Env) {
	InitByFlag(&e)
	InitByEnv(&e)

	if e.appEnvironment == "" {
		e.appEnvironment = "production"
	}
	if e.replayDuration != time.Duration(0*time.Second) && e.replaySecond != 0 {
		e.replayOplog = true
	} else {
		e.replayOplog = false
	}

	if e.memprofile != "" {
		f, err := os.Create(e.memprofile)
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer f.Close()
			tick := time.Tick(time.Duration(20) * time.Second)
			for {
				select {
				case <-tick:
					pprof.WriteHeapProfile(f)
				}
			}
		}()
	}
	return
}

func IsInsertUpdateDelete(op *gtm.Op) bool {
	return isActionableOperation(op.IsInsert, op.IsUpdate, op.IsDelete)
}

func isActionableOperation(filters ...func() bool) bool {
	for _, fn := range filters {
		if fn() {
			return true
		}
	}
	return false
}

// SanitizeData handles type inconsistency between mongo and pg
// and flattens the data from a potentially nested data struct
// into a flattened struct using gjson.
func SanitizeData(c Collection, op *gtm.Op, hasExtraProps bool, isMongoExport bool) (map[string]interface{}, error) {
	if !IsInsertUpdateDelete(op) {
		return nil, nil
	}

	for _, exl := range c.Exclude {
		updateFields, ok := op.UpdateDescription["updatedFields"].(map[string]interface{})
		if !ok {
			break
		}

		if updateFields[exl] != nil {
			return nil, nil
		}
	}

	newData, err := json.Marshal(op.Data)
	if err != nil {
		return nil, err
	}

	parsed := gjson.ParseBytes(newData)
	output := make(map[string]interface{})

	if c.AllField && isMongoExport {
		parsed.ForEach(func(key, value gjson.Result) bool {
			output[key.String()] = value.Value()
			return true // keep iterating
		})
		return output, nil
	}

	for k, v := range c.Fields {
		// Dot notation extraction
		maybe := parsed.Get(k)
		if !maybe.Exists() && v.Export.Type != "JSON" {
			// Fill with nils to ensure that NamedExec works
			output[v.Export.Name] = nil
			continue
		}
		// Sanitize the Value field when it's a map
		value := maybe.Value()
		if !isMongoExport {
			if _, ok := value.(map[string]interface{}); ok {
				// Marshal Objects using JSON
				b, _ := json.Marshal(value)
				output[v.Export.Name] = b
				continue
			}
			if _, ok := value.([]interface{}); ok {
				// Marshal Arrays using JSON
				b, _ := json.Marshal(value)
				output[v.Export.Name] = b
				continue
			}
		}
		output[v.Export.Name] = value
	}

	// Normalize data map to always include the Id with conversion
	// Required for delete actions that have a missing _id field in
	// op.Data. Must occur after the preceeding iterative block
	// in order to avoid being overwritten with nil.
	if op.Id != nil {
		if _, ok := op.Id.(primitive.ObjectID); ok {
			output["_id"] = op.Id.(primitive.ObjectID).Hex()
		} else {
			output["_id"] = op.Id
		}
	}

	if hasExtraProps {
		extraProps := addExtraProps(c.Fields, parsed, isMongoExport)
		if len(extraProps) == 0 {
			output["_extra_props"] = nil
			return output, nil
		}

		if isMongoExport {
			output["_extra_props"] = extraProps
			return output, nil
		}

		jsonExtraProps, _ := json.Marshal(extraProps)
		output["_extra_props"] = jsonExtraProps
	}

	return output, nil
}

func SanitizeDataFile(c Collection, in string, hasExtraProps bool) (map[string]interface{}, error) {
	bytes, err := json.RawMessage(in).MarshalJSON()
	if err != nil {
		return nil, err
	}

	parsed := gjson.ParseBytes(bytes)
	output := make(map[string]interface{})

	for k, v := range c.Fields {
		// Dot notation extraction
		maybe := parsed.Get(k)
		if !maybe.Exists() && v.Export.Type != "JSON" {
			// Fill with nils to ensure that NamedExec works
			output[v.Export.Name] = nil
			continue
		}
		// Sanitize the Value field when it's a map
		value := maybe.Value()
		if d, ok := value.(map[string]interface{}); ok {
			// Marshal Objects using JSON
			if k == "_id" && d["$oid"] != nil {
				output[v.Export.Name] = d["$oid"]
				continue
			}

			b, _ := json.Marshal(value)
			output[v.Export.Name] = b
			continue
		}
		if _, ok := value.([]interface{}); ok {
			// Marshal Arrays using JSON
			b, _ := json.Marshal(value)
			output[v.Export.Name] = b
			continue
		}
		output[v.Export.Name] = value
	}

	if hasExtraProps {
		extraProps := addExtraProps(c.Fields, parsed, false)
		if len(extraProps) == 0 {
			output["_extra_props"] = nil
			return output, nil
		}

		jsonExtraProps, _ := json.Marshal(extraProps)
		output["_extra_props"] = jsonExtraProps
	}

	return output, nil
}

func addExtraProps(pgFields Fields, parsed gjson.Result, isMongoExport bool) map[string]interface{} {
	extraProps := make(map[string]interface{})
	parsed.ForEach(func(key, value gjson.Result) bool {
		if key.String() == "_id" {
			return true
		}

		pgColumnName := pgFields[key.String()].Export.Name
		if len(pgColumnName) == 0 {
			if isMongoExport {
				extraProps[key.String()] = value.Raw
			} else {
				extraProps[key.String()] = json.RawMessage(value.Raw)
			}
		}

		return true // keep iterating
	})
	return extraProps
}

func createFanKey(db string, collection string, export string) string {
	return db + "." + collection + "." + export
}

// EnsureOpHasAllFields: Ensure that required keys are present will null value
func EnsureOpHasAllFields(op *gtm.Op, keysToEnsure []string) *gtm.Op {
	// Guard against assignment into nil map
	if op.Data == nil {
		op.Data = make(map[string]interface{})
	}
	for _, k := range keysToEnsure {
		if _, ok := op.Data[k]; !ok {
			op.Data[k] = nil
		}
	}
	return op
}

func EnsureRightTailType(tailType string) bool {
	if tailType == optLog || tailType == changeStream {
		return true
	}
	return false
}

func HasTypeExport(exportsTo []string, exportType string) bool {
	for _, export := range exportsTo {
		if export == exportType {
			return true
		}
	}
	return false
}

func EnsureRightExport(exports []string) bool {
	counter := 0
	for _, export := range exports {
		if export == mongoExport || export == csvExport || export == postgresExport {
			counter++
		}
	}
	return counter == len(exports)
}

func ExitUnlessValidEnv(e Env) {
	if len(e.configFile) == 0 {
		log.Warnf(`Missing config file path`)
		os.Exit(1)
	}

	exportsTo := strings.Split(e.exports, ",")
	if !EnsureRightExport(exportsTo) {
		log.WithFields(log.Fields{"exports": e.exports}).Warnf(`Export set wrong, just: postgres, csv, mongo`)
		os.Exit(1)
	}

	if e.validatePostgres {
		return
	}

	if e.createTableSQL {
		c := Commands{}
		c.CreateTableSQL()
	}

	if e.urls.mongo == "" && !e.syncFile {
		log.Warnf(`Missing required variable. MONGO_URL must be set. 
		            See the following usage instructions for setting those variables.`)
		os.Exit(1)
	}

	if e.urls.postgres == "" && HasTypeExport(exportsTo, postgresExport) {
		log.Warnf(`Missing required variable. POSTGRES_URL must be set.
					See the following usage instruction for setting those variables.`)
		os.Exit(1)
	}

	if e.urls.mongoExport == "" && HasTypeExport(exportsTo, mongoExport) {
		log.Warnf(`Missing required variable. MONGO_EXPORT_URL must be set.
					See the following usage instruction for setting those variables.`)
		os.Exit(1)
	}

	if !(e.sync || e.tail || e.syncFile) {
		log.WithFields(log.Fields{"sync": e.sync, "tail": e.tail, "sync_file": e.syncFile}).Warnf(`Missing sync or tail or sync file`)
		os.Exit(1)
	}

	if !EnsureRightTailType(e.tailType) {
		log.Warnf(`Tail type set wrong, just: optlog or change-stream`)
		os.Exit(1)
	}
}
