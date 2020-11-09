PROCESS_NUM=$(ps -ef | grep 'ahamove.json' | grep -v "grep" | wc -l)
POSTGRES_URL=""
MONGO_URL=""
# echo "$PROCESS_NUM ahamove mosql running"
if [ $PROCESS_NUM -eq 0 ]
then
     now=$(date +"%F %T")
     #echo "$now mosql ahamove is not running"
     curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=mosql_ahamove_staging&email=thuc@ahamove.com,hungnn@ahamove.com";
     MONGO_URL=$MONGO_URL POSTGRES_URL=$POSTGRES_URL nohup /usr/local/bin/moresql --config-file=/root/go/src/gitlab.com/nnhung1327/moresql/bin/ahamove.json --tail --ssl-insecure-skip-verify --checkpoint --app-name=ahamove --replay-duration=1m > /root/moresql/ahamove.out 2>&1 &
fi

PROCESS_NUM=$(ps -ef | grep 'ahamove_trans' | grep -v "grep" | wc -l)
# echo "$PROCESS_NUM ahamovelog mosql running"
if [ $PROCESS_NUM -eq 0 ]
then
      now=$(date +"%F %T")
      echo "$now mosql ahamove_trans is not running"
      curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=mosql_ahamove_trans_staging&email=hungnn@ahamove.com,thuc@ahamove.com";
      MONGO_URL=$MONGO_URL POSTGRES_URL=$POSTGRES_URL nohup /usr/local/bin/moresql --config-file=/root/go/src/gitlab.com/nnhung1327/moresql/bin/ahamove2.json --tail --ssl-insecure-skip-verify --checkpoint --app-name=ahamove_trans --replay-duration=1m > /root/moresql/ahamove_trans.out 2>&1 &
fi
PROCESS_NUM=$(ps -ef | grep 'supplier_location' | grep -v "grep" | wc -l)
if [ $PROCESS_NUM -eq 0 ]
then
      now=$(date +"%F %T")
      echo "supplier_location is not running"
      curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=moresql_ahamove_trans_staging&email=hungnn@ahamove.com,thuc@ahamove.com";
      MONGO_URL=$(mongo_url) POSTGRES_URL=$(postgres_url) LOG_LEVEL=debug nohup moresql --config-file=/root/go/src/gitlab.com/ahamove/cdc/bin/supplier_location.json --tail --checkpoint --app-name=supplier_location --exports=postgres --tail-type=change-stream --just-insert > /root/moresql/supplier_location.out 2>&1 &
fi
