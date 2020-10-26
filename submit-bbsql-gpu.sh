##submit-bbsql-gpu.sh##

. $SPARK_HOME/conf/spark-env.sh

## BBSQL
export QUERY='Q5'
export DRIVER_MEMORY='10240'
export PARTITIONBYTES='512M'
export PARTITIONS='64'
export BROADCASTTHRESHOLD='512M'

## INPUT_PATH="s3a://path_to_data/data/parquet"
export INPUT_PATH="file:///${MOUNT}/parquet"

## OUTPUT_PATH="s3a://path_to_output/output"
export OUTPUT_PATH="file:///${MOUNT}/results"

## WAREHOUSE_PATH="s3a://path_to_warehouse/warehouse"
export WAREHOUSE_PATH="file:///tmp"

JARS_JAR_NAME=rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar
BBSQL_JAR_NAME=bbsql_apps-0.2.2-SNAPSHOT.jar
JARS_URL="https://cloud.swiftstack.com/v1/AUTH_eric/downloads/rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar"
BBSQL_URL="https://cloud.swiftstack.com/v1/AUTH_eric/downloads/bbsql_apps-0.2.2-SNAPSHOT.jar"
PARQUET_URL="https://cloud.swiftstack.com/v1/AUTH_eric/downloads/1gb-parquet.tar"

mkdir -p $OUTPUT_PATH
mkdir -p $WAREHOUSE_PATH

export JARS=${MOUNT}/bbsql/${JARS_JAR_NAME}
export BBSQL=${MOUNT}/bbsql/${BBSQL_JAR_NAME}

if [ ! -f "${BBSQL}" ]
then
    mkdir -p ${MOUNT}/bbsql && wget -P ${MOUNT}/bbsql -c ${BBSQL_URL} && wget -P ${MOUNT}/bbsql -c ${JARS_URL}

else
    echo "${BBSQL} exists"
fi

if [ ! -d "${MOUNT}/parquet/customer" ]
then
    wget -c ${PARQUET_URL} -O - | sudo tar --strip-components=1 --one-top-level=${MOUNT}/parquet -x

else
    echo "${MOUNT}/parquet/customer exists"
fi

HISTORYPARAMS="--conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:${SPARK_HOME}/history"

S3PARAMS="--conf spark.hadoop.fs.s3a.access.key=${S3A_CREDS_USR} \
        --conf spark.hadoop.fs.s3a.secret.key=${S3A_CREDS_PSW} \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.experimental.input.fadvise=sequential \
        --conf spark.hadoop.fs.s3a.connection.maximum=1000\
        --conf spark.hadoop.fs.s3a.threads.core=1000\
        --conf spark.hadoop.parquet.enable.summary-metadata=false \
        --conf spark.sql.parquet.mergeSchema=false \
        --conf spark.sql.parquet.filterPushdown=true \
        --conf spark.sql.hive.metastorePartitionPruning=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true"

CMDPARAM="--master ${MASTER} \
        --deploy-mode client \
        --jars ${JARS} \
        --num-executors ${NUM_EXECUTORS} \
        --conf spark.cores.max=${TOTAL_CORES} \
        --conf spark.sql.warehouse.dir=${WAREHOUSE_PATH} \
        --driver-memory ${DRIVER_MEMORY}M \
	--conf spark.task.cpus=1 \
        --conf spark.sql.files.maxPartitionBytes=${PARTITIONBYTES} \
        --conf spark.sql.autoBroadcastJoinThreshold=${BROADCASTTHRESHOLD} \
        --conf spark.sql.shuffle.partitions=${PARTITIONS} \
        --conf spark.locality.wait=0s \
        --conf spark.executor.heartbeatInterval=100s \
        --conf spark.network.timeout=3600s \
        --conf spark.storage.blockManagerSlaveTimeoutMs=3600s \
        --conf spark.sql.broadcastTimeout=2000 \
        --conf spark.executor.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
        --conf spark.driver.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
	--conf spark.rapids.sql.variableFloatAgg.enabled=true \
        --conf spark.plugins=com.nvidia.spark.SQLPlugin \
        --conf spark.rapids.sql.concurrentGpuTasks=${CONCURRENTGPU} \
        --conf spark.rapids.memory.gpu.pooling.enabled=true \
        --conf spark.rapids.memory.pinnedPool.size=8g \
        --conf spark.rapids.sql.incompatibleOps.enabled=true \
        --conf spark.rapids.sql.explain=ALL \
        --conf spark.executor.resource.gpu.amount=1 \
        --conf spark.task.resource.gpu.amount=${RESOURCE_GPU_AMT} \
        --conf spark.rapids.sql.batchSizeByte=512M \
        --conf spark.sql.parquet.read.allocation.size=64M \
        --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS \
        ${S3PARAMS}"

$SPARK_HOME/bin/spark-submit --verbose \
        $CMDPARAM \
        --conf spark.executor.extraJavaOptions="-Dai.rapids.cudf.nvtx.enabled=true -Dai.rapids.cudf.prefer-pinned=true -Dai.rapids.spark.semaphore.enabled=true" \
        --class ai.rapids.spark.examples.tpcxbb.Main \
        $BBSQL --xpu=GPU \
        --query="$QUERY" \
        --input="$INPUT_PATH" \
        --output="${OUTPUT_PATH}-gpu/$QUERY"
