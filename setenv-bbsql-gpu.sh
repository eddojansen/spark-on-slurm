##setenv-bbsql-gpu.sh##

## BBSQL
QUERY='Q5'
DRIVER_MEMORY='10240'
PARTITIONBYTES='512M'
PARTITIONS='600'
BROADCASTTHRESHOLD='512M'
TOTAL_CORES=$((${SLURM_CPUS_PER_TASK} * ${SLURM_NTASKS}))
RESOURCE_GPU_AMT=$(echo "scale=3; ${CONCURRENTGPU} * ${SLURM_NTASKS} / $TOTAL_CORES" | bc)
JARS=rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar
BBJAR=bbsql_apps-0.2.2-SNAPSHOT.jar

## INPUT_PATH="s3a://path_to_data/data/parquet"
INPUT_PATH="file:///$MOUNT/parquet"

## OUTPUT_PATH="s3a://path_to_output/output"
OUTPUT_PATH="file:///$MOUNT/results"

## WAREHOUSE_PATH="s3a://path_to_warehouse/warehouse"
WAREHOUSE_PATH="file:///tmp"

if [ ! -d "$MOUNT/parquet/customer" ]
then
    wget -c ${PARQUET_UR} -O - | sudo tar --strip-components=1 --one-top-level=${MOUNT}/parquet -xz

else
    echo "${MOUNT}/parquet/customer exists"
fi

export MASTER="spark://`hostname`:7077"

export HISTORYPARAMS="--conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:$SPARK_HOME/history"

export S3PARAMS="--conf spark.hadoop.fs.s3a.access.key=$S3A_CREDS_USR \
        --conf spark.hadoop.fs.s3a.secret.key=$S3A_CREDS_PSW \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.experimental.input.fadvise=sequential \
        --conf spark.hadoop.fs.s3a.connection.maximum=1000\
        --conf spark.hadoop.fs.s3a.threads.core=1000\
        --conf spark.hadoop.parquet.enable.summary-metadata=false \
        --conf spark.sql.parquet.mergeSchema=false \
        --conf spark.sql.parquet.filterPushdown=true \
        --conf spark.sql.hive.metastorePartitionPruning=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true"

echo "Running query $QUERY maxPartitionBytes:$PARTITIONBYTES autoBroadcastThreshold:$BROADCASTTHRESHOLD shuffle_partition:$PARTITIONS concurrentGpu:$CONCURRENTGPU"

CMDPARAM="--master $MASTER \
        --deploy-mode client \
        --jars $JARS \
        --num-executors $SLURM_NTASKS \
        --conf spark.cores.max=$(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS )) \
        --conf spark.sql.warehouse.dir=$WAREHOUSE_PATH \
        --driver-memory ${DRIVER_MEMORY}M \
	--conf spark.task.cpus=1 \
        --executor-memory $(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))M \
        --conf spark.sql.files.maxPartitionBytes=$PARTITIONBYTES \
        --conf spark.sql.autoBroadcastJoinThreshold=$BROADCASTTHRESHOLD \
        --conf spark.sql.shuffle.partitions=$PARTITIONS \
        --conf spark.locality.wait=0s \
        --conf spark.executor.heartbeatInterval=100s \
        --conf spark.network.timeout=3600s \
        --conf spark.storage.blockManagerSlaveTimeoutMs=3600s \
        --conf spark.sql.broadcastTimeout=2000 \
        --conf spark.executor.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
        --conf spark.driver.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
	--conf spark.rapids.sql.variableFloatAgg.enabled=true \
        --conf spark.plugins=com.nvidia.spark.SQLPlugin \
        --conf spark.rapids.sql.concurrentGpuTasks=$CONCURRENTGPU \
        --conf spark.rapids.memory.gpu.pooling.enabled=true \
        --conf spark.rapids.memory.pinnedPool.size=8g \
        --conf spark.rapids.sql.incompatibleOps.enabled=true \
        --conf spark.executor.extraJavaOptions=`'-Dai.rapids.cudf.nvtx.enabled=true -Dai.rapids.cudf.prefer-pinned=true -Dai.rapids.spark.semaphore.enabled=true'` \
        --conf spark.rapids.sql.explain=ALL \
        --conf spark.executor.resource.gpu.amount=1 \
        --conf spark.task.resource.gpu.amount=$RESOURCE_GPU_AMT \
        --conf spark.rapids.sql.batchSizeByte=512M \
        --conf spark.sql.parquet.read.allocation.size=64M \
        --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS \
        $S3PARAMS"

echo $CONCURRENTGPU
echo ${SPARK_RAPIDS_PLUGIN_JAR}
echo $RESOURCE_GPU_AMT
