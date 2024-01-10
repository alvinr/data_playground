#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Usage: sh get_cat_indices.sh <<file-with-newline-separated-cluster-ids>>"
  exit 1
fi

FILE=$1

for i in $(cat $FILE | tr -d '"'); do
  echo "---"
  echo "Elasticsearch Cluster ID: [$i]"
  tmpfile=$(mktemp /tmp/get_cat_indices.sh.XXXXXX.json)
  printf '{"query":{"bool":{"must":[{"nested":{"path":"resources.elasticsearch","query":{"exists":{"field":"resources.elasticsearch.id"}}}}],"filter":[{"nested":{"path":"resources.elasticsearch","query":{"bool":{"minimum_should_match":1,"should":[{"prefix":{"resources.elasticsearch.id":{"value":"' > $tmpfile
  printf $i >> $tmpfile
  printf '"}}}]}}}}]}}}' >> $tmpfile
  DEPLOYMENT_ID=$(ecl deployment search -f $tmpfile --format "{{.ID}}")
  INDICES_INFO=$(ecl api -H "X-Management-Request: true" -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/indices?h=pri.store.size")
  INDICES=$(echo ${INDICES_INFO[0]})
  NODE_RAM=$(ecl api -H "X-Management-Request: true" -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/nodes?h=ram.max,node.role" | grep -e "h" -e "d" | cut -f 1 -d " ")
  RAM=$(echo ${NODE_RAM[0]})
  echo "$i|${RAM// /,}|${INDICES// /,}" >> cat_indices_output.txt
#  SHARD_COUNT=$(ecl api -H "X-Management-Request: true" -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/shards?h=shard" | sort | uniq | wc -l)
#  echo "$i|$SHARD_COUNT|${RAM// /,}|${INDICES// /,}" >> cat_indices_output.txt
done

