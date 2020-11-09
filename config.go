package moresql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"strings"

	log "github.com/sirupsen/logrus"
)

func LoadConfigString(s string) (Config, error) {
	config := Config{}
	var configDelayed ConfigDelayed
	err := json.Unmarshal([]byte(s), &configDelayed)
	if err != nil {
		log.Fatalln(err)
	}
	for k, v := range configDelayed {
		db := DB{}
		collections := Collections{}
		db.Collections = collections
		schema := "public"
		for k, v := range v.Collections {
			if v.Schema != "" {
				schema = v.Schema
			}
			coll := Collection{Name: v.Name, Schema: schema, ExtraProps: v.ExtraProps, OrderedCols: v.OrderedCols, Exclude: v.Exclude, AllField: v.AllField}
			fields, err := JsonToFields(string(v.Fields))
			if err != nil {
				log.Warnf("JSON Config decoding error: ", err)
				return nil, fmt.Errorf("unable to decode %s", err)
			}
			coll.Fields = fields
			db.Collections[k] = coll
		}
		config[k] = db
	}
	return config, nil
}

func LoadConfig(path string) Config {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		log.WithFields(log.Fields{"path": path, "error": err.Error()}).Error("load_config")
		os.Exit(1)
	}
	config, err := LoadConfigString(string(b))
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("load_config_string")
		os.Exit(1)
	}
	return config
}

func mongoToPostgresTypeConversion(mongoType string) string {
	// Coerce "id" bsonId types into text since Postgres doesn't have type for BSONID
	switch strings.ToLower(mongoType) {
	case "id":
		return "text"
	}
	return mongoType
}

func normalizeDotNotationToPostgresNaming(key string) string {
	re := regexp.MustCompile("\\.")
	return re.ReplaceAllString(key, "_")
}

func JsonToFields(s string) (Fields, error) {
	var init FieldsWrapper
	var err error
	result := Fields{}
	err = json.Unmarshal([]byte(s), &init)
	for k, v := range init {
		field := Field{}
		str := ""
		if err := json.Unmarshal(v, &field); err == nil {
			result[k] = field
		} else if err := json.Unmarshal(v, &str); err == nil {
			// Convert shorthand to longhand Field
			f := Field{
				Mongo{k, str},
				Export{normalizeDotNotationToPostgresNaming(k), mongoToPostgresTypeConversion(str)},
			}
			result[k] = f
		} else {
			errLong := json.Unmarshal(v, &field)
			errShort := json.Unmarshal(v, &str)
			err = fmt.Errorf(fmt.Sprintf("Could not decode Field. Long decoding %+v. Short decoding %+v", errLong, errShort))
			return nil, err
		}
	}
	return result, err
}
