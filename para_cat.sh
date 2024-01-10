#!/bin/bash

trap 'pkill -P $$' SIGINT SIGTERM
if [ $# -eq 0 ]; then
  echo "Usage: bash para_cat.sh <<file-with-newline-separated-cluster-ids>>"
  exit 1
fi

FILE=$(realpath $1)
PREFIX=$(date +%s)_

pushd .; cd /tmp; split -n l/10 $FILE $PREFIX; popd 

for i in /tmp/$PREFIX_*; do
  ./get_cat_indices.sh $i &
done
wait

#rm /tmpi/$PREFIX*

