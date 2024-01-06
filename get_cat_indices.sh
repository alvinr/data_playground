#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Usage: sh get_cat_indices.sh <<file-with-newline-separated-cluster-ids>>"
  exit 1
fi

FILE=$1

for i in $(cat $FILE | tr -d '"'); do
  echo "---"
  echo "Elasticsearch Cluster ID: [$i]"
  printf '{"query":{"bool":{"must":[{"nested":{"path":"resources.elasticsearch","query":{"exists":{"field":"resources.elasticsearch.id"}}}}],"filter":[{"nested":{"path":"resources.elasticsearch","query":{"bool":{"minimum_should_match":1,"should":[{"prefix":{"resources.elasticsearch.id":{"value":"' > filter.json
  printf $i >> filter.json
  printf '"}}}]}}}}]}}}' >> filter.json
  DEPLOYMENT_ID=$(ecl deployment search -f filter.json --format "{{.ID}}")
  INDICES_INFO=$(ecl api -H "X-Management-Request: true" -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/indices?h=pri.store.size")
  INDICES=$(echo ${INDICES_INFO[0]})
  echo "$i,${INDICES// /,}" >> cat_indices_output.txt
done

