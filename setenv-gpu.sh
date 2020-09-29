##setenv-gpu.sh##
export GPU_PER_NODE='1'
export MOUNT=/nfs
export SPARK_HOME=$MOUNT/spark
export PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_CONF_DIR=/nfs/spark/conf
#
export SPARK_RAPIDS_DIR=$MOUNT/sparkRapidsPlugin
export CUDF_JAR_NAME="cudf-0.14-cuda10-1.jar"
export RAPIDS_JAR_NAME="rapids-4-spark_2.12-0.1.0.jar"
export CUDF_FILES_URL="https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar"
export GET_CPU_RES_URL="https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh"
export SPARK_URL="https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz"
export RAPIDS_PLUGIN_URL="https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar"
export CONCURRENTGPU=$GPU_PER_NODE
export WORKER_OPTS="-Dspark.worker.resource.gpu.amount=$CONCURRENTGPU -Dspark.worker.resource.gpu.discoveryScript=$SPARK_RAPIDS_DIR/getGpusResources.sh"
#
export PARQUET_URL="https://cloud.swiftstack.com/v1/AUTH_eric/downloads/100G%20parquet.zip"
#
env=$SPARK_CONF_DIR/spark-env.sh
echo "export SPARK_LOG_DIR=$SPARK_HOME/log" > $env
echo "export SPARK_WORKER_DIR=$SPARK_HOME/sparkworker" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
echo 'export SPARK_WORKER_CORES=`nproc`' >> $env
echo 'export SPARK_WORKER_MEMORY=$(( $SPARK_WORKER_CORES*$SLURM_MEM_PER_CPU ))M' >> $env
#
echo "export CUDF_JAR_NAME=$CUDF_JAR_NAME" >> $env
echo "export RAPIDS_JAR_NAME=$RAPIDS_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_DIR=$SPARK_RAPIDS_DIR" >> $env
echo "export SPARK_CUDF_JAR=$SPARK_RAPIDS_DIR/$CUDF_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_PLUGIN_JAR=$SPARK_RAPIDS_DIR/$RAPIDS_JAR_NAME" >> $env
echo "export SPARK_WORKER_OPTS='"$WORKER_OPTS"'" >> $env
#
echo "export SPARK_HOME=$SPARK_HOME" > ~/.bashrc
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
echo "export SPARK_CONF_DIR=$SPARK_CONF_DIR" >> ~/.bashrc
echo "export RAPIDS_JAR_NAME=$RAPIDS_JAR_NAME" >> ~/.bashrc
echo "export SPARK_RAPIDS_DIR=$SPARK_RAPIDS_DIR" >> ~/.bashrc
echo "export SPARK_CUDF_JAR=$SPARK_RAPIDS_DIR/$CUDF_JAR_NAME" >> ~/.bashrc
echo "export SPARK_RAPIDS_PLUGIN_JAR=$SPARK_RAPIDS_DIR/$RAPIDS_JAR_NAME" >> ~/.bashrc
echo "export CONCURRENTGPU=$CONCURRENTGPU" >> ~/.bashrc

scontrol show hostname $SLURM_JOB_NODELIST > $SPARK_CONF_DIR/slaves

conf=$SPARK_CONF_DIR/spark-defaults.conf
echo "spark.default.parallelism" $(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS ))> $conf
echo "spark.submit.deployMode" client >> $conf
echo "spark.master" spark://`hostname`:7077 >> $conf
echo "spark.executor.cores" $SLURM_CPUS_PER_TASK >> $conf
echo "spark.executor.memory" $(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))M >> $conf

## BBSQL

export DRIVER_MEMORY="10240"
export QUERY="Q5"
export PARTITIONBYTES="512M"
export PARTITIONS="600"
export BROADCASTTHRESHOLD="512M"
export NUM_EXECUTOR_CORES=$(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS ))
export RESOURCE_GPU_AMT=$(( $CONCURRENTGPU * $SLURM_NTASKS / $NUM_EXECUTOR_CORES ))

# If you don't have UCX in your environment select 0 for all your runs
# 1 for rc, 2 for tcp, 0 for no ucx
export UCX_SELECT='0'

export JARS=rapids-4-spark-integration-tests_2.12-0.1-SNAPSHOT.jar

## INPUT_PATH="s3a://path_to_data/data/parquet"
export INPUT_PATH="file:///$MOUNT/parquet"

## OUTPUT_PATH="s3a://path_to_output/output"
export OUTPUT_PATH="file:///$MOUNT/results"

## WAREHOUSE_PATH="s3a://path_to_warehouse/warehouse"
export WAREHOUSE_PATH="file:///tmp"

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

if [ $UCX_SELECT == 0 ]; then
       export  UCX_PARAMS=""
elif [ $UCX_SELECT == 1 ]; then
       export UCX_PARAMS="--conf spark.shuffle.manager=ai.rapids.spark.RapidsShuffleManager \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.rapids.shuffle.transport.enabled=true \
        --conf spark.rapids.memory.host.spillStorageSize=32GB \
        --conf spark.executorEnv.UCX_ERROR_SIGNALS= \
        --conf spark.executorEnv.UCX_MAX_RNDV_RAILS=1 \
        --conf spark.executorEnv.UCX_MEMTYPE_CACHE=n \
        --conf spark.executorEnv.UCX_RNDV_SCHEME=put_zcopy \
        --conf spark.executorEnv.UCX_ZCOPY_THRESH=0 \
        --conf spark.executorEnv.UCX_BCOPY_THRESH=inf \
        --conf spark.executorEnv.UCX_RNDV_THRESH=0 \
        --conf spark.rapids.shuffle.maxMetadataSize=1MB \
        --conf spark.executorEnv.UCX_TLS=cuda_copy,cuda_ipc,rc,tcp"
else
        export UCX_PARAMS="--conf spark.shuffle.manager=ai.rapids.spark.RapidsShuffleManager \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.rapids.shuffle.transport.enabled=true \
        --conf spark.rapids.memory.host.spillStorageSize=32GB \
        --conf spark.executorEnv.UCX_ERROR_SIGNALS= \
        --conf spark.executorEnv.UCX_MAX_RNDV_RAILS=1 \
        --conf spark.executorEnv.UCX_MEMTYPE_CACHE=n \
        --conf spark.executorEnv.UCX_RNDV_SCHEME=put_zcopy \
        --conf spark.executorEnv.UCX_ZCOPY_THRESH=0 \
        --conf spark.executorEnv.UCX_BCOPY_THRESH=inf \
        --conf spark.executorEnv.UCX_RNDV_THRESH=0 \
        --conf spark.rapids.shuffle.maxMetadataSize=1MB \
        --conf spark.executorEnv.UCX_TLS=cuda_copy,cuda_ipc,tcp"
fi

export CMDPARAMS="--master $MASTER \
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
        --conf spark.executor.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
        --conf spark.driver.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR}:/opt/ucx/lib \
        $S3PARAMS"
