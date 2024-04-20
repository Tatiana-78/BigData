#!/bin/bash

OUT_DIR=$1
NUM_REDUCERS=2

# Delete previous directory
hdfs dfs -rm -r -skipTrash $OUT_DIR*

# Running yarn jar
yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="chernova_mapreduce_task1" \
    -D mapreduce.job.reducer=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "./mapper.py" \
    -reducer "./reducer.py" \
    -input /data/yelp/business \
    -output $OUT_DIR

# Checking the results
for num in `seq 0 $(($NUM_REDUCERS - 1))`
do
    hdfs dfs -cat ${OUT_DIR}/part-0000$num | head
done
