if [ $(docker inspect -f '{{.State.Running}}' ahamove) = "false" ]; then
  curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=moresql_ahamove_staging&email=thuc@ahamove.com,hungnn@ahamove.com";
  docker restart ahamove
fi

if [ $(docker inspect -f '{{.State.Running}}' ahamove_trans) = "false" ]; then
  curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=moresql_trans_ahamove_staging&email=thuc@ahamove.com,hungnn@ahamove.com";
  docker restart ahamove_trans
fi

if [ $(docker inspect -f '{{.State.Running}}' supplier_location) = "false" ]; then
  curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=supplier_location_ahamove_staging&email=thuc@ahamove.com,hungnn@ahamove.com";
  docker restart supplier_location
fi