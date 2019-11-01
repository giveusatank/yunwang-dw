/usr/local/spark-2.2.3-bin-hadoop2.6/bin/spark-submit  \
--class com.pep.ods.history.Extractor \
--master yarn-client \
--driver-memory=2G \
--num-executors=100 \
--executor-cores=1 \
--executor-memory=16000m \
--conf spark.default.parallelism=300 \
--conf spark.yarn.executor.memoryOverhead=2500m \
--conf spark.memory.fraction=0.9 \
--conf spark.memory.storageFraction=0.2 \
--conf spark.yarn.maxAppAttempts=2 \
 \
--conf spark.shuffle.consolidateFile=true \
--conf spark.shuffle.io.maxRetries=5 \
--conf spark.shuffle.io.retryWait=4s \
--conf spark.network.timeout=360s \
--conf spark.locality.wait=10ms \
--conf spark.reducer.maxSizeInFlight=24m \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC  -XX:MaxTenuringThreshold=15 -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:NewRatio=1 -XX:ParallelGCThreads=4" \
--queue dwq \
 /root/extractor_log/yunwang-dw-1.0-SNAPSHOT.jar \
2018-09-01 2018-09-10 8000000000 20000000 