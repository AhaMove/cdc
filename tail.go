package moresql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"time"

	"strconv"

	"github.com/jmoiron/sqlx"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/paulbellamy/ratecounter"
	"github.com/rwynn/gtm"
	"github.com/serialx/hashring"
	log "github.com/sirupsen/logrus"
	"github.com/thejerf/suture"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Tailer is the core struct for performing
// Mongo->Pg streaming.
type Tailer struct {
	config Config
	pg     *sqlx.DB
	// session    *mgo.Session
	client       *mongo.Client
	clientExport *mongo.Client
	env          Env
	counters     map[string]counters
	stop         chan bool
	fan          map[string]chan Op
	checkpoint   *cmap.ConcurrentMap
}

type Op struct {
	data   *gtm.Op
	export string
}

// Stop is the func necessary to terminate action
// when using Suture library
func (t *Tailer) Stop() {
	fmt.Println("Stopping service")
	t.stop <- true
}

func (t *Tailer) startOverflowConsumers(c <-chan Op) {
	exportSize := len(strings.Split(t.env.exports, ","))
	for i := 1; i <= workerCountOverflow*exportSize; i++ {
		go t.consumer(strconv.Itoa(i), c, nil)
	}
}

type EpochTimestamp int64

func now() time.Time {
	return time.Unix(0, time.Now().UnixNano()/1e6*1e6)
}

func BuildOptionAfterFromTimestamp(timestamp EpochTimestamp, replayDuration time.Duration) (func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error), error) {
	if timestamp != EpochTimestamp(0) && int64(timestamp) < time.Now().Unix() {
		// We have a starting oplog entry
		f := func() time.Time { return time.Unix(int64(timestamp), 0) }
		return OpTimestampWrapper(f, time.Duration(0)), nil
	}
	if replayDuration != time.Duration(0) {
		return OpTimestampWrapper(now, replayDuration), nil
	}
	return OpTimestampWrapper(now, time.Duration(0)), nil
}

func (t *Tailer) NewOptions(timestamp EpochTimestamp, replayDuration time.Duration) (*gtm.Options, error) {
	options := gtm.DefaultOptions()
	after, err := BuildOptionAfterFromTimestamp(timestamp, replayDuration)
	if err != nil {
		return nil, err
	}
	actual, _ := after(nil, nil)
	epoch, _ := gtm.ParseTimestamp(actual)
	log.WithFields(log.Fields{"app_name": t.env.appName}).Infof("Starting from epoch: %+v", epoch)
	options.After = after
	options.BufferSize = 500
	options.BufferDuration = time.Duration(500 * time.Millisecond)
	options.Ordering = gtm.Document
	options.Filter = func(op *gtm.Op) bool {
		return op.Namespace != "config.system.sessions"
	}
	return options, nil
}

func (t *Tailer) ChangeStreamOptions(op *gtm.Options) {
	if len(t.fan) > 0 {
		var changeStream []string
		for k := range t.fan {
			s := strings.Split(k, ".")
			changeStream = append(changeStream, s[0]+"."+s[1])
		}
		op.ChangeStreamNs = changeStream
		op.OpLogDisabled = true
	}
}

func (t *Tailer) NewFan() map[string]chan Op {
	fan := make(map[string]chan Op)
	// Register Channels
	for dbName, db := range t.config {
		for collectionName := range db.Collections {
			for _, export := range strings.Split(t.env.exports, ",") {
				fan[createFanKey(dbName, collectionName, export)] = make(chan Op, 1000)
			}
		}
	}
	return fan
}

func consistentBroker(in chan Op, ring *hashring.HashRing, workerPool map[string]chan Op) {
	for {
		select {
		case op := <-in:
			node, ok := ring.GetNode(fmt.Sprintf("%s", op.data.Id))
			if !ok {
				log.Error("Failed at getting worker node from hashring")
			} else {
				out := workerPool[node]
				out <- op
			}
		}
	}
}

func (t *Tailer) startDedicatedConsumers(fan map[string]chan Op, overflow chan Op) {
	// Reserved workers for individual channels
	for k, c := range fan {
		workerPool := make(map[string]chan Op)
		var workers [workerCount]int
		for i := range workers {
			o := make(chan Op)
			workerPool[strconv.Itoa(i)] = o
		}
		keys := []string{}
		for k := range workerPool {
			keys = append(keys, k)
		}
		ring := hashring.New(keys)
		wg.Add(1)
		go consistentBroker(c, ring, workerPool)
		for k, workerChan := range workerPool {
			go t.consumer(k, workerChan, overflow)
		}
		log.WithFields(log.Fields{
			"count":      workerCount,
			"collection": k,
		}).Debug("Starting worker(s)")
	}
}

type MoresqlMetadata struct {
	AppName     string    `db:"app_name" bson:"app_name"`
	LastEpoch   int64     `db:"last_epoch" bson:"last_epoch"`
	ProcessedAt time.Time `db:"processed_at" bson:"processed_at"`
}

func NewTailer(config Config, pg *sqlx.DB, client *mongo.Client, env Env, clientExport *mongo.Client) *Tailer {
	checkpoint := cmap.New()
	initCounters := make(map[string]counters)
	for _, export := range strings.Split(env.exports, ",") {
		initCounters[export] = buildCounters(export)
	}
	return &Tailer{config: config, pg: pg, client: client, env: env, stop: make(chan bool), counters: initCounters, checkpoint: &checkpoint, clientExport: clientExport}
}

func (t *Tailer) FetchMetadata() (metadata MoresqlMetadata) {
	if !t.env.checkpoint {
		metadata.LastEpoch = 0
		return
	}

	if HasTypeExport(strings.Split(t.env.exports, ","), mongoExport) {
		for db := range t.config {
			collection := t.clientExport.Database(db).Collection("moresql_metadata")
			err := collection.FindOne(context.TODO(), bson.M{"app_name": t.env.appName}).Decode(&metadata)
			if err != nil {
				log.Errorf("Error while reading moresql_metadata table %+v", err)
			}
		}
		return
	}

	q := Queries{}
	err := t.pg.Get(&metadata, q.GetMetadata(), t.env.appName)
	// No rows means this is first time with table
	if err != nil && err != sql.ErrNoRows {
		log.Errorf("Error while reading moresql_metadata table %+v", err)
		c := Commands{}
		c.CreateTableSQL()
	}
	return
}

type gtmTail struct {
	ops  gtm.OpChan
	errs chan error
}

func (t *Tailer) Read() {
	metadata := t.FetchMetadata()

	var lastEpoch int64
	if t.env.replaySecond != 0 {
		lastEpoch = int64(t.env.replaySecond)
	} else {
		lastEpoch = metadata.LastEpoch
	}
	options, err := t.NewOptions(EpochTimestamp(lastEpoch), t.env.replayDuration)
	if t.env.tailType == changeStream {
		t.ChangeStreamOptions(options)
	}
	if err != nil {
		log.Fatal(err.Error())
	}
	ops, errs := gtm.Tail(t.client, options)
	g := gtmTail{ops, errs}
	go func() {
		for {
			select {
			case <-t.stop:
				return
			case err := <-g.errs:
				if matched, _ := regexp.MatchString("i/o timeout", err.Error()); matched {
					// Restart gtm.Tail
					// Close existing channels to not leak resources
					log.Errorf("Problem connecting to mongo initiating reconnection: %s", err.Error())
					close(g.ops)
					close(g.errs)
					latest, ok := t.checkpoint.Get("latest")
					if ok && latest != nil {
						metadata = latest.(MoresqlMetadata)
						lastEpoch = metadata.LastEpoch
						options, err := t.NewOptions(EpochTimestamp(lastEpoch), t.env.replayDuration)
						if err != nil {
							log.Fatal(err.Error())
						}
						ops, errs = gtm.Tail(t.client, options)
						fmt.Printf("ops: %+v\n", ops)
						g = gtmTail{ops, errs}
					} else {
						log.Fatalf("Exiting: Unable to recover from %s", err.Error())
					}
				} else {
					log.Fatalf("Exiting: Mongo tailer returned error %s", err.Error())
				}
			case op := <-g.ops:
				// Check if we're watching for the collection
				db := op.GetDatabase()
				coll := op.GetCollection()
				for _, export := range strings.Split(t.env.exports, ",") {
					t.counters[export].read.Incr(1)
					log.WithFields(log.Fields{
						"operation":  op.Operation,
						"collection": op.GetCollection(),
						"id":         op.Id,
						"export":     export,
					}).Debug("Received operation")
					key := createFanKey(db, coll, export)
					if c := t.fan[key]; c != nil {
						collection := t.config[db].Collections[coll]
						o := Statement{collection}
						data := EnsureOpHasAllFields(op, o.mongoFields())
						c <- Op{data, export}
					} else {
						t.counters[export].skipped.Incr(1)
						log.Debug("Missing channel for this collection")
					}
				}
				for k, v := range t.fan {
					if len(v) > 0 {
						log.Debugf("Channel %s has %d", k, len(v))
					}
				}
			}
		}
	}()
}

func (t *Tailer) Write() {
	t.fan = t.NewFan()
	log.WithField("struct", t.fan).Debug("Fan")
	overflow := make(chan Op)
	t.startDedicatedConsumers(t.fan, overflow)
	t.startOverflowConsumers(overflow)
}

func (t *Tailer) Report() {
	c := time.Tick(time.Duration(reportFrequency) * time.Second)
	go func() {
		for {
			select {
			case <-c:
				t.ReportCounters()
			}
		}
	}()

}

func (t *Tailer) SaveCheckpoint(m MoresqlMetadata, database string) error {
	exports := strings.Split(t.env.exports, ",")
	if HasTypeExport(exports, mongoExport) {
		collection := t.clientExport.Database(database).Collection("moresql_metadata")
		result, err := collection.UpdateOne(
			context.TODO(),
			bson.M{"app_name": m.AppName},
			bson.D{{"$set", m}},
			options.Update().SetUpsert(true))
		if err != nil {
			log.Errorf("Unable to save into moresql_metadata: %+v, %+v", result, err.Error())
		}
		return err
	}

	q := Queries{}
	result, err := t.pg.NamedExec(q.SaveMetadata(), m)
	if err != nil {
		log.Errorf("Unable to save into moresql_metadata: %+v, %+v", result, err.Error())
	}
	return err
}

func (t *Tailer) Checkpoints() {
	go func() {
		timer := time.Tick(checkpointFrequency)
		for {
			select {
			case _ = <-timer:
				latest, ok := t.checkpoint.Get("latest")
				database, success := t.checkpoint.Get("database")
				if ok && latest != nil && database != nil && success {
					t.SaveCheckpoint(latest.(MoresqlMetadata), database.(string))
					log.Infof("Saved checkpointing %+v", latest.(MoresqlMetadata))
				}
			}
		}
	}()
}

// Serve is the func necessary to start action
// when using Suture library
func (t *Tailer) Serve() {
	t.Write()
	t.Read()
	t.Report()
	if t.env.checkpoint {
		t.Checkpoints()
	}
	<-t.stop
}

type counters struct {
	name    string
	insert  *ratecounter.RateCounter
	update  *ratecounter.RateCounter
	delete  *ratecounter.RateCounter
	read    *ratecounter.RateCounter
	skipped *ratecounter.RateCounter
}

func (c *counters) All() map[string]*ratecounter.RateCounter {
	cx := make(map[string]*ratecounter.RateCounter)
	cx["insert"] = c.insert
	cx["update"] = c.update
	cx["delete"] = c.delete
	cx["read"] = c.read
	cx["skipped"] = c.skipped
	return cx
}

func buildCounters(name string) counters {
	return counters{
		name,
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
		ratecounter.NewRateCounter(1 * time.Minute),
	}
}

func (t *Tailer) ReportCounters() {
	for _, export := range strings.Split(t.env.exports, ",") {
		counter := t.counters[export]
		for i, c := range counter.All() {
			log.Infof("Counter %s - Rate of %s per min: %d", export, i, c.Rate())
		}
	}
}

func (t *Tailer) MsLag(epoch uint32, nowFunc func() time.Time) int64 {
	// TODO: use time.Duration instead of this malarky
	ts := time.Unix(int64(epoch), 0)
	d := nowFunc().Sub(ts)
	nanoToMillisecond := func(t time.Duration) int64 { return t.Nanoseconds() / 1e6 }
	return nanoToMillisecond(d)
}

func (t *Tailer) consumer(id string, in <-chan Op, overflow chan<- Op) {
	var workerType string
	if overflow != nil {
		workerType = "Dedicated"
	} else {
		workerType = "Generic"
	}
	for {
		if overflow != nil && len(in) > workerCount {
			// Siphon off overflow
			select {
			case op := <-in:
				overflow <- op
			}
			continue
		}
		select {
		case op := <-in:
			t.processOp(op, workerType)
			if t.env.checkpoint {
				t.checkpoint.Set("database", op.data.GetDatabase())
				t.checkpoint.Set("latest", t.OpToMoresqlMetadata(op.data))
			}
		}
	}
}

func (t *Tailer) OpToMoresqlMetadata(op *gtm.Op) MoresqlMetadata {
	ts, _ := gtm.ParseTimestamp(op.Timestamp)
	return MoresqlMetadata{AppName: t.env.appName, ProcessedAt: time.Now(), LastEpoch: int64(ts)}
}

func (t *Tailer) processOp(op Op, workerType string) {
	collectionName := op.data.GetCollection()
	db := op.data.GetDatabase()
	st := FullSyncer{Config: t.config}
	o, c := st.statementFromDbCollection(db, collectionName)
	isMongoExport := op.export == mongoExport
	data, err := SanitizeData(c, op.data, len(c.ExtraProps) > 0, isMongoExport)
	if err != nil {
		log.WithFields(log.Fields{"collection": collectionName,"error": err, "data": op.data.Data}).Fatal("Error SanitizeData")
	}

	if data["id"] == nil {
		return
	}

	if op.export == csvExport {
		t.exportCSV(op.data, c, data)
	}

	if op.export == postgresExport {
		t.exportPostgres(o, op.data, data, workerType)
	}

	if isMongoExport {
		t.exportMongo(o, op.data, data, workerType)
	}
}

func OpTimestampWrapper(f func() time.Time, ago time.Duration) func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error) {
	return func(*mongo.Client, *gtm.Options) (primitive.Timestamp, error) {
		now := f()
		inPast := now.Add(-ago)
		var c uint32 = 1
		return NewMongoTimestamp(inPast, c)
	}
}

func Tail(config Config, pg *sqlx.DB, env Env, client *mongo.Client, clientExport *mongo.Client) {
	supervisor := suture.NewSimple("Supervisor")
	service := NewTailer(config, pg, client, env, clientExport)
	supervisor.Add(service)
	supervisor.ServeBackground()
	<-service.stop
}
