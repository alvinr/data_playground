#!/bin/bash

trap 'pkill -P $$' SIGINT SIGTERM
if [ $# -eq 0 ]; then
  echo "Usage: bash para_cat.sh <<file-with-newline-separated-cluster-ids>> <<outputfile>>"
  exit 1
fi

PARTS=20
FILE=$(realpath $1)
PREFIX=$(date +%s)_

OUT=$PWD/"cat_indices_output.txt"
if [ $# -eq 2 ]; then
  OUT=$(realpath $2)
fi

pushd .; cd /tmp; split -n l/$PARTS $FILE $PREFIX; popd 

for i in /tmp/$PREFIX_*; do
  ./get_cat_indices.sh $i $i.out &
done
wait

cat /tmp/$PREFIX_*.out > $OUT

#rm /tmp/$PREFIX*

