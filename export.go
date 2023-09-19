package moresql

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/rwynn/gtm"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getTypeField(fields map[string]Field, field string) string {
	for _, v := range fields {
		if v.Export.Name == field {
			return v.Export.Type
		}
	}
	return ""
}

func (t *Tailer) exportCSV(op *gtm.Op, coll Collection, data map[string]interface{}) {
	clientsFile, _ := os.OpenFile(t.env.csvPathFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)

	// Layout and GMT+7
	layout := "2006-01-02 15:04:05Z"
	loc := time.FixedZone("UTC+7", 7*60*60)

	w := csv.NewWriter(clientsFile)
	var record []string
	for _, v := range coll.OrderedCols {
		if data[v] == nil {
			break
		}
		switch vv := data[v].(type) {
		case float64:
			if getTypeField(coll.Fields, v) == "TIMESTAMP" {
				timestamp := time.Unix(int64(vv), 0).In(loc).Format(layout)
				record = append(record, timestamp)
				continue
			}
			record = append(record, fmt.Sprintf("%f", vv))

		case string:
			record = append(record, fmt.Sprintf("%s", vv))
		default:
			record = append(record, fmt.Sprintf("%v", vv))
		}
	}
	if len(record) != len(coll.OrderedCols) {
		return
	}

	w.Write(record)
	w.Flush()

	switch {
	case op.IsInsert():
		t.counters[csvExport].insert.Incr(1)
	case op.IsUpdate():
		t.counters[csvExport].update.Incr(1)
	default:
		t.counters[csvExport].skipped.Incr(1)
	}
}

func (t *Tailer) exportPostgres(o Statement, op *gtm.Op, data map[string]interface{}, workerType string) {
	payload := map[string]interface{}{
		"action":     op.Operation,
		"collection": op.GetCollection(),
		"timestamp":  op.Timestamp,
		"data":       data,
	}

	if t.env.justInsert {
		t.counters[postgresExport].insert.Incr(1)
		log.WithFields(log.Fields{
			"data":  data,
			"query": o.BuildInsert(),
		}).Debug("just insert")
		_, err := t.pg.NamedExec(o.BuildInsert(), data)
		t.logFn(err, workerType, payload)
		return
	}

	switch {
	case op.IsInsert():
		t.counters[postgresExport].insert.Incr(1)
		log.WithFields(log.Fields{
			"data":  data,
			"query": o.BuildUpsert(),
		}).Debug("insert")
		_, err := t.pg.NamedExec(o.BuildUpsert(), data)
		t.logFn(err, workerType, payload)
	case op.IsUpdate():
		t.counters[postgresExport].update.Incr(1)
		log.WithFields(log.Fields{
			"data":  data,
			"query": o.BuildUpsert(),
		}).Debug("update")
		_, err := t.pg.NamedExec(o.BuildUpsert(), data)
		t.logFn(err, workerType, payload)
	case op.IsDelete() && t.env.allowDeletes:
		t.counters[postgresExport].delete.Incr(1)
		log.WithFields(log.Fields{
			"data":  data,
			"query": o.BuildDelete(),
		}).Debug("delete")
		_, err := t.pg.NamedExec(o.BuildDelete(), data)
		t.logFn(err, workerType, payload)
	default:
		t.counters[postgresExport].skipped.Incr(1)
	}
}

func (t *Tailer) exportMongo(op *gtm.Op, data map[string]interface{}, workerType string) {
	collection := t.clientExport.Database(op.GetDatabase()).Collection(op.GetCollection())
	payload := map[string]interface{}{
		"action":     op.Operation,
		"collection": op.GetCollection(),
		"database":   op.GetDatabase(),
		"timestamp":  op.Timestamp,
		"data":       data,
	}

	delete(data, "_id")

	if t.env.justInsert {
		t.counters[mongoExport].insert.Incr(1)
		// delete(data, "_id")
		_, err := collection.InsertOne(context.TODO(), data)
		t.logFn(err, workerType, payload)
		return
	}

	switch {
	case op.IsInsert():
		t.counters[mongoExport].insert.Incr(1)
		_, err := collection.UpdateOne(
			context.Background(),
			bson.M{"_id": op.Id},
			bson.D{{"$set", data}},
			options.Update().SetUpsert(true))
		t.logFn(err, workerType, payload)
	case op.IsUpdate():
		t.counters[mongoExport].update.Incr(1)
		_, err := collection.UpdateOne(
			context.Background(),
			bson.M{"_id": op.Id},
			bson.D{{"$set", data}},
			options.Update().SetUpsert(true))
		t.logFn(err, workerType, payload)
	case op.IsDelete() && t.env.allowDeletes:
		t.counters[mongoExport].delete.Incr(1)
		_, err := collection.DeleteOne(context.Background(), bson.M{"_id": op.Id})
		t.logFn(err, workerType, payload)
	default:
		t.counters[mongoExport].skipped.Incr(1)
	}
}

func (t *Tailer) logFn(e error, workerType string, payload map[string]interface{}) {
	if e != nil {
		ts1, ts2 := gtm.ParseTimestamp(payload["timestamp"].(primitive.Timestamp))
		gtmLag := t.MsLag(ts1, time.Now)
		log.WithFields(log.Fields{
			"ts":      ts1,
			"ts2":     ts2,
			"msLag":   gtmLag,
			"now":     time.Now().Unix(),
			"payload": payload,
			"error":   e,
		}).Error(fmt.Sprintf("%s worker processed", workerType))
		if !t.env.skipError {
			os.Exit(1)
		}
	}
}
