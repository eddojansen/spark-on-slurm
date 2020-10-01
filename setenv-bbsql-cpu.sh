##setenv-bbsql-cpu.sh##

## BBSQL
export DRIVER_MEMORY='10240'
export QUERY='Q5'
export PARTITIONBYTES='512M'
export PARTITIONS='600'
export BROADCASTTHRESHOLD='512M'

export JARS=rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar
export BBJAR=bbsql_apps-0.2.2-SNAPSHOT.jar

## INPUT_PATH="s3a://path_to_data/data/parquet"
export INPUT_PATH="file:///$MOUNT/parquet"

## OUTPUT_PATH="s3a://path_to_output/output"
export OUTPUT_PATH="file:///$MOUNT/results"

## WAREHOUSE_PATH="s3a://path_to_warehouse/warehouse"
export WAREHOUSE_PATH="file:///tmp"

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

export BBSQLCMDPARAMS="--master $MASTER \
        --deploy-mode client \
        --jars $JARS \
        --num-executors $SLURM_NTASKS \
        --conf spark.cores.max=$(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS )) \
        --conf spark.sql.warehouse.dir=$WAREHOUSE_PATH \
        --driver-memory ${DRIVER_MEMORY}M \
        --conf spark.sql.files.maxPartitionBytes=$PARTITIONBYTES \
        --conf spark.sql.autoBroadcastJoinThreshold=$BROADCASTTHRESHOLD \
        --conf spark.sql.shuffle.partitions=$PARTITIONS \
        --conf spark.locality.wait=0s \
        --conf spark.executor.heartbeatInterval=100s \
        --conf spark.network.timeout=3600s \
        --conf spark.storage.blockManagerSlaveTimeoutMs=3600s \
        --conf spark.sql.broadcastTimeout=2000 \
	--class ai.rapids.spark.examples.tpcxbb.Main \
        $S3PARAMS \
        $BBJAR --xpu=CPU \
        --query="$QUERY" \
        --input="$INPUT_PATH" \
        --output="${OUTPUT_PATH}-cpu/$QUERY""

