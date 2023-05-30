#!/usr/bin/env bash
export SPARK_SUBMIT_VERSION=3.0

DATE="20230501"
JAR=$(ls target/scala-2.12/antispame_offline-assembly-0.1.0.jar)

doas spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.version=3.0 \
  --num-executors 128 \
  --executor-cores 8 \
  --executor-memory 16G \
  --driver-memory 16G \
  --driver-cores 16 \
  --queue root.koala_yg_ad_antispam_strategy \
  --conf spark.hadoop.mapred.output.compress=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.yarn.executor.memoryOverhead=32g \
  --conf spark.yarn.driver.memoryOverhead=16g \
  --conf spark.driver.maxResultSize=16g \
  --conf spark.shuffle.io.maxRetries=5 \
  --conf spark.blacklist.enabled=true \
  --conf spark.shuffle.spill.compress=true \
  --conf spark.shuffle.hdfs.enabled=true \
  --conf spark.speculation.multiplier=5 \
  --conf spark.shuffle.service.enabled=true \
  --conf hive.auto.convert.join=true \
  --conf spark.shuffle.hdfs.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.adaptiveFileSplit=true \
  --conf spark.shuffle.hdfs.mergeIndexAndData.enabled=true \
  --conf spark.sql.adaptive.maxNumPostShufflePartitions=20000 \
  --conf spark.dynamicAllocation.maxExecutors=128 \
  --conf spark.dynamicAllocation.minExecutors=64 \
  --conf spark.dynamicAllocation.initialExecutors=64 \
  --conf spark.vcore.boost.ratio=2 \
  --conf spark.default.parallelism=20000 \
  --conf spark.sql.hive.caseSensitiveInferenceMode=NEVER_INFER \
  --conf spark.scheduler.listenerbus.eventqueue.capacity=200000 \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.kryoserializer.buffer=512m \
  --conf spark.sql.broadcastTimeout=36000 \
  --conf spark.network.timeout=8000 \
  --conf spark.akka.timeout=8000 \
  --conf spark.rpc.askTimeout=8000 \
  --conf spark.sql.autoBroadcastJoinThreshold=2048m \
  --conf spark.sql.tungsten.enabled=true \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.io.compression.codec="snappy" \
  --conf spark.sql.parquet.compression.codec="snappy" \
  --class "group.GroupStrategyMiningExecutor" \
  --files "./conf/req_group2.xml" \
  $JAR \
  --date $DATE \
  --group-config "req_group2.xml" \
  --group_name "REQ_GROUP_DEVICE_RESOURCE_V1"
