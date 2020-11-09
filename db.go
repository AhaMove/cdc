package moresql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetMongoConnection(url string) *mongo.Client {
	clientOptions := options.Client().ApplyURI(url)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func GetPostgresConnection(env Env) (pg *sqlx.DB) {
	var err error
	pg, err = sqlx.Connect("postgres", env.urls.postgres)
	if err != nil {
		log.Fatal(err)
	}
	setupPgDefaults(pg, env.postgresMaxOpenConns)
	return
}

// setupPgDefaults: Set safe cap so workers do not overwhelm server
func setupPgDefaults(pg *sqlx.DB, numberOfConnection int) {
	pg.SetMaxIdleConns(numberOfConnection)
	pg.SetMaxOpenConns(numberOfConnection)
}

// Credit: https://github.com/go-mgo/mgo/pull/202/files#diff-b47d6566744e81abad9312022bdc8896R374
// From @mwmahlberg
func NewMongoTimestamp(t time.Time, c uint32) (primitive.Timestamp, error) {
	var tv uint32
	u := t.Unix()
	if u < 0 || u > math.MaxUint32 {
		return primitive.Timestamp{}, errors.New("invalid value for time")
	}
	tv = uint32(u)
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, tv)
	binary.Write(&buf, binary.BigEndian, c)
	i := int64(binary.BigEndian.Uint64(buf.Bytes()))
	ts, ordinal := ParseTimestamp(i)
	return primitive.Timestamp{T: uint32(ts), I: uint32(ordinal)}, nil
}

func ParseTimestamp(timestamp int64) (int32, int32) {
	ordinal := (timestamp << 32) >> 32
	ts := (timestamp >> 32)
	return int32(ts), int32(ordinal)
}
