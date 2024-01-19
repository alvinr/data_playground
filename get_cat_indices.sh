#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Usage: sh get_cat_indices.sh <<file-with-newline-separated-cluster-ids>> <<outputfile>>"
  exit 1
fi

FILE=$1
OUT=$PWD/"cat_indices_output.txt"
if [ $# -eq 2 ]; then
  OUT=$2
fi

tmpjson=$(mktemp $FILE.XXXXXX.json)
tmpout=$(mktemp $FILE.XXXXXX.txt)

for i in $(cat $FILE | tr -d '"'); do
  echo "---"
  echo "Elasticsearch Cluster ID: [$i]"
  printf '{"query":{"bool":{"must":[{"nested":{"path":"resources.elasticsearch","query":{"exists":{"field":"resources.elasticsearch.id"}}}}],"filter":[{"nested":{"path":"resources.elasticsearch","query":{"bool":{"minimum_should_match":1,"should":[{"prefix":{"resources.elasticsearch.id":{"value":"' > $tmpjson
  printf $i >> $tmpjson
  printf '"}}}]}}}}]}}}' >> $tmpjson
  DEPLOYMENT_ID=$(ecl deployment search -f $tmpjson --format "{{.ID}}")
  INDEXES=$(ecl api -H "X-Management-Request: true" -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/indices?pri=true&h=index,pri,pri.store.size&bytes=mb" | tr '\n' ';' | tr -s ' ' | tr ' ' ',')
  RAM=$(ecl api -H 'X-Management-Request: true' -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/nodes?h=ram.max,node.role&bytes=mb" | grep -e "h" -e "d" | cut -f 1 -d " " | tr -s " ")
  RAM=$(echo ${RAM[0]})
  VERSION=$(ecl api -H 'X-Management-Request: true' -X GET "v1/deployments/$DEPLOYMENT_ID/elasticsearch/main-elasticsearch/proxy/_cat/nodes?h=version" | head -1)
  echo "$i|$VERSION|${RAM// /,}|$INDEXES" >> $tmpout
done

cat $tmpout >> $OUT

