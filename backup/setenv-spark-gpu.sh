##setenv-spark-gpu.sh##

## Set the mountpoint used for spark installation and applications
export MOUNT=/data

## Set CONCURRENTGPU, meaning the amount of threads per GPU
export CONCURRENTGPU='1'

export RESOURCE_GPU_AMOUNT=$((${SLURM_GPUS} / ${SLURM_JOB_NUM_NODES}))
export TOTAL_CORES=$((${SLURM_CPUS_PER_TASK} * ${SLURM_NTASKS}))
export RESOURCE_GPU_AMT=$(echo "scale=3; ${SLURM_GPUS} / ${TOTAL_CORES}" | bc)
export NUM_EXECUTORS=$SLURM_GPUS
export EXECUTORS_CORES=$((${TOTAL_CORES} / ${NUM_EXECUTORS}))
export EXECUTOR_MEMORY=$((( ${SLURM_CPUS_PER_TASK} * ${SLURM_MEM_PER_CPU} / ${RESOURCE_GPU_AMOUNT})))M
export SPARK_RAPIDS_DIR=$MOUNT/sparkRapidsPlugin
export WORKER_OPTS="-Dspark.worker.resource.gpu.amount=${RESOURCE_GPU_AMOUNT} -Dspark.worker.resource.gpu.discoveryScript=${SPARK_RAPIDS_DIR}/getGpusResources.sh"
export SPARK_HOME=$MOUNT/spark
export SPARK_LOG_DIR=$SPARK_HOME/log
export SPARK_WORKER_DIR=$SPARK_HOME/sparkworker
export PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

## Update JAR names and download URL's
CUDF_JAR_NAME="cudf-0.14-cuda10-1.jar"
RAPIDS_JAR_NAME="rapids-4-spark_2.12-0.1.0.jar"
CUDF_FILES_URL="https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar"
GET_CPU_RES_URL="https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh"
SPARK_URL="https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz"
RAPIDS_PLUGIN_URL="https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar"

mkdir -p $MOUNT/sparkRapidsPlugin

if [ ! -d "$SPARK_HOME/sbin" ]
then
    wget -c ${SPARK_URL} -O - | sudo tar --strip-components=1 --one-top-level=${SPARK_HOME} -xz
else
    echo "${SPARK_HOME} exists"
fi

sudo mkdir -p $SPARK_LOG_DIR
sudo mkdir -p $SPARK_WORKER_DIR
sudo chown -R $(id -u):$(id -g) ${MOUNT}/spark

if [ ! -f "${MOUNT}/sparkRapidsPlugin/getGpusResources.sh" ]
then
    wget -P ${MOUNT}/sparkRapidsPlugin -c ${GET_CPU_RES_URL} && chmod +x ${MOUNT}/sparkRapidsPlugin/getGpusResources.sh
else
    echo "getGpusResources.sh exists"
fi

if [ ! -f "${MOUNT}/sparkRapidsPlugin/${CUDF_JAR_NAME}" ]
then
    wget -P ${MOUNT}/sparkRapidsPlugin -c ${CUDF_FILES_URL}
else
    echo "${CUDF_JAR_NAME} exists"
fi

if [ ! -f "${MOUNT}/sparkRapidsPlugin/${RAPIDS_JAR_NAME}" ]
then
    wget -P ${MOUNT}/sparkRapidsPlugin -c ${RAPIDS_PLUGIN_URL}
else
    echo "${RAPIDS_JAR_NAME} exists"
fi

env=$SPARK_HOME/conf/spark-env.sh
echo "export SPARK_LOG_DIR=$SPARK_LOG_DIR" > $env
echo "export SPARK_WORKER_DIR=$SPARK_WORKER_DIR" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
#echo 'export SPARK_WORKER_CORES=`nproc`' >> $env
echo "export SPARK_WORKER_CORES=$SLURM_CPUS_PER_TASK" >> $env
echo 'export SPARK_WORKER_MEMORY=$(( $SPARK_WORKER_CORES*$SLURM_MEM_PER_CPU ))M' >> $env

echo "export CUDF_JAR_NAME=$CUDF_JAR_NAME" >> $env
echo "export RAPIDS_JAR_NAME=$RAPIDS_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_DIR=$SPARK_RAPIDS_DIR" >> $env
echo "export SPARK_CUDF_JAR=$SPARK_RAPIDS_DIR/$CUDF_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_PLUGIN_JAR=$SPARK_RAPIDS_DIR/$RAPIDS_JAR_NAME" >> $env
echo "export SPARK_WORKER_OPTS='"$WORKER_OPTS"'" >> $env

sudo chmod +x $SPARK_HOME/conf/spark-env.sh
echo "export SPARK_HOME=$MOUNT/spark" > ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin"  >> ~/.bashrc
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc

scontrol show hostname $SLURM_JOB_NODELIST > $SPARK_HOME/conf/slaves

conf=$SPARK_HOME/conf/spark-defaults.conf
#echo "spark.default.parallelism" $(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS )) > $conf
echo "spark.default.parallelism" $SLURM_GPUS > $conf
echo "spark.submit.deployMode" client >> $conf
echo "spark.master" spark://`hostname`:7077 >> $conf
echo "spark.executor.cores" $((${TOTAL_CORES}/${NUM_EXECUTORS})) >> $conf
echo "spark.executor.memory" $EXECUTOR_MEMORY >> $conf
echo "spark.driver.memory" 10g  >> $conf
echo "spark.driver.maxResultSize" 10g  >> $conf
