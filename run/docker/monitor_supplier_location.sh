if [ $(docker inspect -f '{{.State.Running}}' supplier_location) = "false" ]; then
  curl -s -O "https://admin.ahamove.com/public/v1/health/alert?target=supplier_location_ahamove_staging&email=thuc@ahamove.com,hungnn@ahamove.com";
  docker restart supplier_location
fi