##setenv.sh##
#module load JAVA/jdk1.8.0_31 spark
export MOUNT=/quobyte/config
export SPARK_HOME=$MOUNT/spark
export PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_CONF_DIR=~/SparkConf
mkdir -p $SPARK_CONF_DIR
mkdir -p $MOUNT/sparkRapidsPlugin
#
export SPARK_RAPIDS_DIR=$MOUNT/sparkRapidsPlugin
export CUDF_JAR_NAME="cudf-0.14-cuda10-1.jar"
export RAPIDS_JAR_NAME="rapids-4-spark_2.12-0.1.0.jar"
export CUDF_FILES_URL="https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar"
export WORKER_OPTS="-Dspark.worker.resource.gpu.amount=1"
export GET_CPU_RES_URL="https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh"
export SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz"
export RAPIDS_PLUGIN_URL="https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar"

#
env=$SPARK_CONF_DIR/spark-env.sh
echo "export SPARK_LOG_DIR=~/SparkLog" > $env
echo "export SPARK_WORKER_DIR=~/SparkWorker" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
echo 'export SPARK_WORKER_CORES=`nproc`' >> $env
echo 'export SPARK_WORKER_MEMORY=$(( $SPARK_WORKER_CORES*$SLURM_MEM_PER_CPU ))M' >> $env
#
#echo "export ADDRS="nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g'"" >> $env
echo "export CUDF_JAR_NAME=$CUDF_JAR_NAME" >> $env
echo "export RAPIDS_JAR_NAME=$RAPIDS_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_DIR=$SPARK_RAPIDS_DIR" >> $env
echo "export SPARK_CUDF_JAR=$SPARK_RAPIDS_DIR/$CUDF_JAR_NAME" >> $env
echo "export SPARK_RAPIDS_PLUGIN_JAR=$SPARK_RAPIDS_DIR/$RAPIDS_JAR_NAME" >> $env
echo 'export SPARK_WORKER_OPTS="'$WORKER_OPTS'"' >> $env
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
