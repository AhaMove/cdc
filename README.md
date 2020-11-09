# MoreSQL

## Introduction

Customizing from [moresql](https://github.com/zph/moresql), add \_extra_props, export to CSV, postgres using optlog or change stream

# Usage

## Setup

```
go get -u gitlab.com/ahamove/cdc
```

This is private project, if local, use can basic auth, ssh key
Or clone project

## Prerequisite

### Create config is just same mosql

### Convert yml to json

```
cd go/src/go/src/gitlab.com/ahamove/cdc
ruby ./bin/convert_config_from_mosql_moresql ./bin/{file_name}.yml ./bin/{file_name}.json
```

In /bin has some example

### Setup env variable

```
cd go/src/go/src/gitlab.com/ahamove/cdc
make setup_moresql
```

### Create database in postgres

```
cd go/src/go/src/gitlab.com/ahamove/cdc
MONGO_URL="" POSTGRES_URL="" moresql -config-file=./bin/{file_name}.json -validate
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

### Full Sync

Note: Just save into postgres

```

MONGO_URL="" POSTGRES_URL="" cmds/moresql/main.go -config-file=./bin/{file_name}.json -full-sync
```
