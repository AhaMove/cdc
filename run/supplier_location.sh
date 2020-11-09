POSTGRES_URL="postgresql://doadmin:i6g87966roxc074p@aha-staging-do-user-1538085-0.db.ondigitalocean.com:25060/ahamove?sslmode=require" \ 
MONGO_URL="mongodb+srv://admin:Aha2020@ahamove-sz8j3.mongodb.net/ahamove" \
nohup moresql --config-file=/mnt/volume_airflow3/cdc/bin/supplier_location.json --tail=true \ 
--checkpoint=true -app-name=supplier_location_csv --exports=csv --tail-type=change-stream \
--csv-path-file=/mnt/volume_airflow3/archive/gs/supplier_location_moresql/supplier_location.csv \ 
> /mnt/volume_airflow3/archive/gs/supplier_location_moresql/supplier_location.out 2>&1 &