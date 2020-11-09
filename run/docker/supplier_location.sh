docker run -d --name supplier_location \
-e 'POSTGRES_URL=postgresql://doadmin:n2swhb5bk8mx8f63@postgresql-do-do-user-1538085-0.db.ondigitalocean.com:25060/ahamove?sslmode=require' \
-e 'MONGO_URL=mongodb+srv://mosql:fn6U4ZZIFiRNu8fH@ahamove-sjm3i.mongodb.net/ahamove?readPreference=secondaryPreferred&ssl=true&replicaSet=AhaMove-shard-0&authSource=admin' \
-e 'TILE38_EXPORT_URL=178.128.52.160:9851' \
-e 'TAIL=true' \
-e 'CONFIG_FILE=/app/bin/supplier_location.json' \
-e 'LOG_LEVEL=info' \
-e 'TAIL_TYPE=change-stream' \
-e 'APP_NAME=supplier_location_csv' \
-e 'CHECK_POINT=true' \
-e 'EXPORTS=csv' \
-e 'CSV_PATH_FILE=/app/data/supplier_location.csv' \
-v /mnt/volume_airflow3/archive/gs/supplier_location_moresql/log:/var/log/cdc \
-v /mnt/volume_airflow3/cdc/bin:/app/bin \
-v /mnt/volume_airflow3/archive/gs/supplier_location_moresql:/app/data \
asia.gcr.io/aha-move/cdc