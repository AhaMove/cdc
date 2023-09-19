# CDC: change data capture

## Introduction

Customizing from [moresql](https://github.com/zph/moresql), add \_extra_props, exclude field, export to CSV, postgres, mongodb using optlog or change stream.
Beside, Dockerlize project and sync data from json file to postgres.

# Usage

## Prerequisite

### Create config is just same mosql

1. To Postgres, csv

```
{db_name}: # name db in mongodb
  {collection_name}: # name collection in mongodb
    :columns:
    - id: # field in sync db
      :source: _id  # field mongodb
      :type: TEXT
    - time:
      :source: time
      :type: DOUBLE PRECISION
    - status:
      :source: status
      :type: JSONB
    - create_time:
      :source: create_time
      :type: DOUBLE PRECISION
    - received_time:
      :source: received_time
      :type: DOUBLE PRECISION
    - assign_type:
      :source: assign_type
      :type: TEXT
    - delivery:
      :source: delivery
      :type: TEXT
    :meta:
      :table: order_notify # (required) name table in synced db
      :schema: custom # (option) if using pg, default public. Ex: custom.order_notify
      :extra_props: JSONB # (option) if not define any above fields, other is added in _extra_props field
      :condition_field: delivery # (option) this field allows filtering data when streaming, the field must be defined in "columns" above
      :condition_value: ahamove # (option) this value of the condition_field field allows filtering data when streaming, only support equal compare at the present
    :exclude: # (option) if received data has below fields, it will skip
      - time
      - assign_type
```

b. To mongo

```
{db_name}:
    {collection_name}:
        :meta:
        :table: testing
        :all_field: true
```

For specific fields, using example in postgres or csv

### Convert yml to json

```
ruby ./bin/convert_config_from_mosql_moresql ./bin/{file_name}.yml ./bin/{file_name}.json
```

### Create database in postgres

```
MONGO_URL="" POSTGRES_URL="" cmds/moresql/main.go -config-file=./bin/{file_name}.json -validate
```

- After validate: 2 cases

### Missing table

```sql
-- Execute the following SQL to setup table in Postgres. Replace $USERNAME with the moresql user.
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
```

Just copy and run them in postgres

### Existing Table

```
Validation succeeded. Postgres tables look good.
```

## Basic Use

### Tail

1. Export to CSV

```
MONGO_URL="" POSTGRES_URL="" cmds/moresql/main.go -config-file=./bin/{file_name}.json -tail --app-name={app_name} --checkpoint --csv-export --tail-type={optlog|change-stream} --csv-path-file=""
```

2. Save into postgres

```
MONGO_URL="" POSTGRES_URL="" cmds/moresql/main.go -config-file=./bin/{file_name}.json --tail --app-name={app_name} --checkpoint --tail-type={optlog|change-stream}
```

Run background

```
MONGO_URL=$MONGO_URL POSTGRES_URL=$POSTGRES_URL LOG_LEVEL=info LOG_PATH=$LOG_PATH nohup moresql --config-file={path_to_bin}/{config_name}.json --tail --checkpoint --app-name={app_name} --tail-type=change-stream --allow-deletes=false --replay-duration=20m > {path_to_save_logg}/{log_name}.out 2>&1 &
```

3. Save into mongo

```
MONGO_URL="" MONGO_EXPORT_URL="" EXPORTS=mongo go run cmds/moresql/main.go --tail --checkpoint --app-name={app_name} --tail-type=change-stream --allow-deletes=false --config-file=./bin/{file_name}.json
```

### Full Sync

Note: Just save into postgres

```

MONGO_URL="" POSTGRES_URL="" go run cmds/moresql/main.go -config-file=./bin/{file_name}.json --full-sync
```

### Sync File to PG

```
go run cmds/moresql/main.go --sync-file --sync-file-path={path_to_file_sync}.json --sync-file-collection={pg_table_name} --sync-file-database={pg_database_name} -config-file=./bin/{file_name}.json
```

## Dockerlize

1. Build image

```
docker build -t {name_image} . --rm
```

2. Start

```
docker run -d --name {name_container} \
-e 'POSTGRES_URL=' \
-e 'MONGO_URL=' \
-e 'TAIL=1' \
-e 'CONFIG_FILE=/app/bin/{file_config}.json' \
-e 'LOG_LEVEL=info' \
-e 'TAIL_TYPE=change-stream' \
-e 'APP_NAME={app_name}' \
-e 'CHECK_POINT=true' \
-v {path_to_save_log}:/var/log/cdc \
-v {path_to_bin}:/app/bin \
{image_name}
```
