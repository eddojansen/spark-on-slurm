##setenv-cpu.sh##
export MOUNT=/nfs
export SPARK_HOME=$MOUNT/spark
export PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_CONF_DIR=/nfs/spark/conf
export SPARK_URL="https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz"
#
env=$SPARK_CONF_DIR/spark-env.sh
echo "export SPARK_LOG_DIR=$SPARK_HOME/log" > $env
echo "export SPARK_WORKER_DIR=$SPARK_HOME/sparkworker" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
echo 'export SPARK_WORKER_CORES=`nproc`' >> $env
echo 'export SPARK_WORKER_MEMORY=$(( $SPARK_WORKER_CORES*$SLURM_MEM_PER_CPU ))M' >> $env
#
echo "export SPARK_HOME=$SPARK_HOME" > ~/.bashrc
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
echo "export SPARK_CONF_DIR=$SPARK_CONF_DIR" >> ~/.bashrc

scontrol show hostname $SLURM_JOB_NODELIST > $SPARK_CONF_DIR/slaves

conf=$SPARK_CONF_DIR/spark-defaults.conf
echo "spark.default.parallelism" $(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS ))> $conf
echo "spark.submit.deployMode" client >> $conf
echo "spark.master" spark://`hostname`:7077 >> $conf
echo "spark.executor.cores" $SLURM_CPUS_PER_TASK >> $conf
echo "spark.executor.memory" $(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))M >> $conf

## BBSQL
DRIVER_MEMORY=10240
QUERY="Q5"
PARTITIONBYTES='512M'
PARTITIONS='600'
BROADCASTTHRESHOLD='512M'

JARS=rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar

## INPUT_PATH="s3a://path_to_data/data/parquet"
INPUT_PATH="file:///$MOUNT/parquet"
mkdir -p $MOUNT/parquet

## OUTPUT_PATH="s3a://path_to_output/output"
OUTPUT_PATH="file:///$MOUNT/results"
mkdir -p $MOUNT/results

## WAREHOUSE_PATH="s3a://path_to_warehouse/warehouse"
WAREHOUSE_PATH="file:///tmp"

MASTER="spark://`hostname`:7077"

HISTORYPARAMS="--conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:$SPARK_HOME/history"

S3PARAMS="--conf spark.hadoop.fs.s3a.access.key=$S3A_CREDS_USR \
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

CMDPARAMS="--master $MASTER \
        --deploy-mode client \
        --jars $JARS \
        --num-executors $SLURM_NTASKS \
        --conf spark.cores.max=$(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS )) \
        --conf spark.sql.warehouse.dir=$WAREHOUSE_PATH \
        --executor-memory $(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU-$DRIVER_MEMORY ))M \
	--driver-memory ${DRIVER_MEMORY}M \
        --conf spark.sql.files.maxPartitionBytes=$PARTITIONBYTES \
        --conf spark.sql.autoBroadcastJoinThreshold=$BROADCASTTHRESHOLD \
        --conf spark.sql.shuffle.partitions=$PARTITIONS \
        --conf spark.locality.wait=0s \
        --conf spark.executor.heartbeatInterval=100s \
        --conf spark.network.timeout=3600s \
        --conf spark.storage.blockManagerSlaveTimeoutMs=3600s \
        --conf spark.sql.broadcastTimeout=2000 \
        $S3PARAMS"
