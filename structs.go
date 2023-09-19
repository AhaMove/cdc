package moresql

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type DBResult struct {
	MongoDB    string
	Collection string
	Data       map[string]interface{}
}

type MongoResult struct {
	DB struct {
		Source      string
		Destination string
	}
	Data map[string]interface{}
}

type urls struct {
	mongo       string
	mongoExport string
	postgres    string
}

type Env struct {
	urls                  urls
	sync                  bool
	syncFile              bool
	syncFilePath          string
	syncFileCollection    string
	syncFileDatabase      string
	tail                  bool
	tailType              string
	SSLCert               string
	SSLInsecureSkipVerify bool
	configFile            string
	allowDeletes          bool
	monitor               bool
	replayOplog           bool
	replayDuration        time.Duration
	replaySecond          int64
	checkpoint            bool
	appName               string
	createTableSQL        bool
	validatePostgres      bool
	reportingToken        string
	appEnvironment        string
	errorReporting        string
	memprofile            string
	postgresMaxOpenConns  int
	exports               string
	csvPathFile           string
	justInsert            bool
	skipError             bool
}

func (e *Env) UseSSL() (r bool) {
	r = false
	if e.SSLCert != "" || e.SSLInsecureSkipVerify {
		r = true
	}
	return
}

// Queries contains the sql commands used by Moresql
type Queries struct{}

// GetMetadata fetches the most recent metadata row for this appname
func (q *Queries) GetMetadata() string {
	return `SELECT * FROM moresql_metadata WHERE app_name=$1 ORDER BY last_epoch DESC LIMIT 1;`
}

// SaveMetadata performs an upsert using metadata with uniqueness constraint on app_name
func (q *Queries) SaveMetadata() string {
	return `INSERT INTO "moresql_metadata" ("app_name", "last_epoch", "processed_at")
VALUES (:app_name, :last_epoch, :processed_at)
ON CONFLICT ("app_name")
DO UPDATE SET "last_epoch" = :last_epoch, "processed_at" = :processed_at;`
}

// CreateMetadataTable provides the sql required to setup the metadata table
func (q *Queries) CreateMetadataTable() string {
	return `
-- create the moresql_metadata table for checkpoint persistance
CREATE TABLE public.moresql_metadata
(
    app_name TEXT NOT NULL,
    last_epoch INT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);
-- Setup mandatory unique index
CREATE UNIQUE INDEX moresql_metadata_app_name_uindex ON public.moresql_metadata (app_name);

-- Grant permissions to this user, replace $USERNAME with moresql's user
GRANT SELECT, UPDATE, DELETE ON TABLE public.moresql_metadata TO $USERNAME;

COMMENT ON COLUMN public.moresql_metadata.app_name IS 'Name of application. Used for circumstances where multiple apps stream to same PG instance.';
COMMENT ON COLUMN public.moresql_metadata.last_epoch IS 'Most recent epoch processed from Mongo';
COMMENT ON COLUMN public.moresql_metadata.processed_at IS 'Timestamp for when the last epoch was processed at';
COMMENT ON TABLE public.moresql_metadata IS 'Stores checkpoint data for MoreSQL (mongo->pg) streaming';
`
}

func (q *Queries) GetColumnsFromTable() string {
	return `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = :schema
  AND table_name   = :table`
}

func (q *Queries) GetTableColumnIndexMetadata() string {
	return `
-- Get table, columns, and index metadata
WITH tables_and_indexes AS (
  -- CREDIT: http://stackoverflow.com/a/25596855
    SELECT
      c.relname                                       AS table,
      f.attname                                       AS column,
      pg_catalog.format_type(f.atttypid, f.atttypmod) AS type,
      f.attnotnull                                    AS notnull,
      i.relname                                       AS index_name,
      CASE
      WHEN i.oid <> 0
        THEN TRUE
      ELSE FALSE
      END                                             AS is_index,
      CASE
      WHEN p.contype = 'p'
        THEN TRUE
      ELSE FALSE
      END                                             AS primarykey,
      CASE
      WHEN p.contype = 'u'
        THEN TRUE
      WHEN p.contype = 'p'
        THEN TRUE
      ELSE FALSE
      END                                             AS uniquekey,
      CASE
      WHEN f.atthasdef = 't'
        THEN d.adsrc
      END                                             AS default
    FROM pg_attribute f
      JOIN pg_class c ON c.oid = f.attrelid
      JOIN pg_type t ON t.oid = f.atttypid
      LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
      LEFT JOIN pg_class AS g ON p.confrelid = g.oid
      LEFT JOIN pg_index AS ix ON f.attnum = ANY (ix.indkey) AND c.oid = f.attrelid AND c.oid = ix.indrelid
      LEFT JOIN pg_class AS i ON ix.indexrelid = i.oid

    WHERE c.relkind = 'r' :: CHAR
          AND n.nspname = 'public'  -- Replace with Schema name
          --AND c.relname = 'nodes'  -- Replace with table name, or Comment this for get all tables
          AND f.attnum > 0
    ORDER BY c.relname, f.attname
)
SELECT count(*) from tables_and_indexes
WHERE "table" = $1
AND "column" = $2
AND is_index IS TRUE
-- TODO: determine how to check if index is unique vs unique column
-- AND uniquekey IS TRUE;
	`
}

type Commands struct{}

func (c *Commands) CreateTableSQL() {
	q := Queries{}
	fmt.Print("-- Execute the following SQL to setup table in Postgres. Replace $USERNAME with the moresql user.")
	fmt.Println(q.CreateMetadataTable())
	os.Exit(0)
}

type ColumnResult struct {
	Name string `db:"column_name"`
}

type TableColumn struct {
	Schema   string
	Table    string
	Column   string
	Type     string
	Message  string
	Solution string
}

func (t *TableColumn) uniqueIndex() string {
	return fmt.Sprintf("CREATE UNIQUE INDEX %s_service_uindex_on_%s ON %s.%s (%s);", t.Table, t.Column, t.Schema, t.Table, t.Column)
}

func (t *TableColumn) createColumn() string {
	return fmt.Sprintf(`ALTER TABLE %s.%s ADD %s %s NULL;`, t.Schema, t.Table, normalizeDotNotationToPostgresNaming(t.Column), t.Type)
}

type hasUniqueIndex struct {
	Value int `db:"count"`
}

func (h *hasUniqueIndex) isValid() bool {
	if h.Value > 0 {
		return true
	}
	return false
}

func (c *Commands) ValidateTablesAndColumns(config Config, pg *sqlx.DB) {
	q := Queries{}
	missingColumns := []TableColumn{}
	// Validates configuration of Postgres based on config file
	// Only validates SELECT and column existance
	for _, db := range config {
		for _, coll := range db.Collections {
			table := coll.Name
			// TODO: allow for non-public schema
			schema := coll.Schema
			// Check that all columns are present
			rows, err := pg.NamedQuery(q.GetColumnsFromTable(), map[string]interface{}{"schema": schema, "table": table})
			if err != nil {
				log.Error(err)
			}
			// TODO: add validation that column types equal the types present in config

			resultMap := make(map[string]string)
			for rows.Next() {
				var row ColumnResult
				err := rows.StructScan(&row)
				if err != nil {
					log.Fatalln(err)
				}
				resultMap[row.Name] = row.Name
			}

			for _, orderField := range coll.OrderedCols {
				for _, field := range coll.Fields {
					k := field.Export.Name
					if orderField == k {
						_, ok := resultMap[k]
						if ok != true {
							t := TableColumn{Schema: schema, Table: table, Column: k, Message: "Missing Column", Type: field.Export.Type}
							t.Solution = t.createColumn()
							missingColumns = append(missingColumns, t)
						}
					}
				}
			}

			if len(coll.ExtraProps) > 0 {
				_, ok := resultMap["_extra_props"]
				if !ok {
					t := TableColumn{Schema: schema, Table: table, Column: "_extra_props", Message: "Missing Column", Type: coll.ExtraProps}
					t.Solution = t.createColumn()
					missingColumns = append(missingColumns, t)
				}
			}

			// Check that each table has _id as in a unique index
			r := hasUniqueIndex{}
			err = pg.Get(&r, q.GetTableColumnIndexMetadata(), table, "id")
			if err != nil {
				log.Error(err)
			}

			if r.isValid() == false {
				t := TableColumn{Schema: schema, Table: table, Column: "id", Message: "Missing Unique Index on Column", Type: ""}
				t.Solution = t.uniqueIndex()
				missingColumns = append(missingColumns, t)
			}

		}
	}
	if len(missingColumns) != 0 {
		log.Print("The following errors were reported:")
		tables := make(map[string]TableColumn)
		for _, v := range missingColumns {
			log.Printf("Table %s.%s Column: %s, Error: %s", v.Schema, v.Table, v.Column, v.Message)
			tables[v.Table] = v
		}
		log.Printf("SQL Output to assist with correcting table schema malformation:")

		// Table level advice
		// CREATE TABLE IF NOT EXISTS public.distributions();
		for _, v := range tables {
			fmt.Printf("CREATE TABLE IF NOT EXISTS %s.%s();\n", v.Schema, v.Table)
		}

		// Column level advice
		for _, v := range missingColumns {
			fmt.Printf("%s\n", v.Solution)
		}
		os.Exit(1)
	}
	log.Printf("Validation succeeded. Postgres tables look good.")
	os.Exit(0)
}

type Mongo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type Export struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// nameQuoted is required for postgres table names
// and field names in case they conflict with SQL
// builtin functions
func (p Export) nameQuoted() string {
	return fmt.Sprintf(`"%s"`, p.Name)
}

type Field struct {
	Mongo  Mongo  `json:"mongo"`
	Export Export `json:export`
}
type Fields map[string]Field
type FieldShorthand map[string]string
type FieldsWrapper map[string]json.RawMessage

type Collection struct {
	Name        string   	`json:"name"`
	Schema      string   	`json:"schema"`
	Fields      Fields   	`json:"fields"`
	ExtraProps  string   	`json:"extra_props"`
	OrderedCols []string 	`json:"ordered_cols"`
	Exclude     []string 	`json:"exclude"`
	AllField    bool     	`json:"all_field"`
	ConditionField string `json:"condition_field"`
	ConditionValue string `json:"condition_value"`
}

type CollectionDelayed struct {
	Name        string          `json:"name"`
	Schema      string          `json:"schema"`
	Fields      json.RawMessage `json:"fields"`
	ExtraProps  string          `json:"extra_props"`
	OrderedCols []string        `json:"ordered_cols"`
	Exclude     []string        `json:"exclude"`
	AllField    bool            `json:"all_field"`
	ConditionField string 			`json:"condition_field"`
	ConditionValue string 			`json:"condition_value"`
}

func (c Collection) pgTableQuoted() string {
	return fmt.Sprintf(`%s."%s"`, c.Schema, c.Name)
}

type DBDelayed struct {
	Collections CollectionsDelayed `json:"collections"`
}
type DB struct {
	Collections Collections `json:"collections"`
}

type Collections map[string]Collection
type CollectionsDelayed map[string]CollectionDelayed

// Config provides the core struct for
// the ultimate unmarshalled moresql.json

type Config map[string]DB

// ConfigDelayed provides lazy config loading
// to support shorthand and longhand variants
type ConfigDelayed map[string]DBDelayed

// Statement provides functions for building up upsert/insert/update/allowDeletes
// sql commands appropriate for a gtm.Op.Data
type Statement struct {
	Collection Collection
}

func (o *Statement) prefixColon(s string) string {
	return fmt.Sprintf(":%s", s)
}

func (o *Statement) mongoFields() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Mongo.Name)
	}
	return fields
}

func (o *Statement) postgresFields() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Export.Name)
	}
	return fields
}

func (o *Statement) postgresFieldsQuoted() []string {
	var fields []string
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		fields = append(fields, v.Export.nameQuoted())
	}
	return fields
}

func (o *Statement) postgresExtraPropsQuoted(fields []string) []string {
	if len(o.Collection.ExtraProps) > 0 {
		fields = append(fields, fmt.Sprintf(`"%s"`, "_extra_props"))
	}
	return fields
}

func (o *Statement) colonFields() []string {
	var withColons []string
	for _, f := range o.postgresFields() {
		withColons = append(withColons, o.prefixColon(f))
	}
	return withColons
}

func (o *Statement) colonExtraProps(withColons []string) []string {
	if len(o.Collection.ExtraProps) > 0 {
		withColons = append(withColons, o.prefixColon("_extra_props"))
	}
	return withColons
}

func (o *Statement) joinedPlaceholders() string {
	return strings.Join(o.colonExtraProps(o.colonFields()), ", ")
}

func (o *Statement) joinLines(sx ...string) string {
	return strings.Join(sx, "\n")
}

func (o *Statement) buildAssignment() string {
	set := []string{}
	for _, k := range o.sortedKeys() {
		v := o.Collection.Fields[k]
		if k != "_id" {
			// Accesses data that has already been sanitized into postgres naming
			set = append(set, fmt.Sprintf(`%s = :%s`, v.Export.nameQuoted(), v.Export.Name))
		}
	}
	if len(o.Collection.ExtraProps) > 0 {
		set = append(set, fmt.Sprintf(`"%s" = :%s`, "_extra_props", "_extra_props"))
	}
	return strings.Join(set, ", ")
}

func (o *Statement) sortedKeys() []string {
	var keys []string
	for k := range o.Collection.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (o *Statement) id() Field {
	return o.Collection.Fields["_id"]
}

func (o *Statement) whereById() string {
	id := o.id()
	return fmt.Sprintf(`WHERE %s = :%s`, id.Export.nameQuoted(), id.Mongo.Name)
}

func (o *Statement) BuildUpsert() string {
	insert := o.BuildInsert()
	onConflict := fmt.Sprintf("ON CONFLICT (%s)", o.id().Export.nameQuoted())
	doUpdate := fmt.Sprintf("DO UPDATE SET %s;", o.buildAssignment())
	output := o.joinLines(insert, onConflict, doUpdate)
	return output
}

func (o *Statement) BuildInsert() string {
	fields := o.postgresExtraPropsQuoted(o.postgresFieldsQuoted())
	insertInto := fmt.Sprintf("INSERT INTO %s (%s)", o.Collection.pgTableQuoted(), strings.Join(fields, ", "))
	values := fmt.Sprintf("VALUES (%s)", o.joinedPlaceholders())
	output := o.joinLines(insertInto, values)
	return output
}

func (o *Statement) BuildUpdate() string {
	update := fmt.Sprintf("UPDATE %s", o.Collection.pgTableQuoted())
	set := fmt.Sprintf("SET %s", o.buildAssignment())
	where := fmt.Sprintf("%s;", o.whereById())
	return o.joinLines(update, set, where)
}

func (o *Statement) BuildDelete() string {
	return fmt.Sprintf("DELETE FROM %s %s;", o.Collection.pgTableQuoted(), o.whereById())
}
