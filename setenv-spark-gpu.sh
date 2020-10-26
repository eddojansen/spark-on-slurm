##setenv-spark-gpu.sh##
set -x
export SPARK_CONF_DIR=/data/SparkConf
mkdir -p $SPARK_CONF_DIR

## Set concurrent GPU's meaning the amount threads per GPU
export CONCURRENTGPU='4'

export GPU_PER_NODE=$(( ${SLURM_GPUS} / ${SLURM_JOB_NUM_NODES} ))
export TOTAL_CORES=$(( ${SLURM_CPUS_PER_TASK} * ${SLURM_NTASKS} ))
export NUM_EXECUTORS=$SLURM_GPUS
export NUM_EXECUTOR_CORES=$(( ${TOTAL_CORES} / ${NUM_EXECUTORS} ))
export RESOURCE_GPU_AMT=$(echo "scale=3; ${NUM_EXECUTORS} / ${TOTAL_CORES}" | bc)
export WORKER_OPTS="-Dspark.worker.resource.gpu.amount=$GPU_PER_NODE -Dspark.worker.resource.gpu.discoveryScript=$SPARK_RAPIDS_DIR/getGpusResources.sh"


env=$SPARK_CONF_DIR/spark-env.sh
echo "export SPARK_LOG_DIR=$SPARK_HOME/log" > $env
echo "export SPARK_WORKER_DIR=$SPARK_HOME/sparkworker" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
echo "export SPARK_WORKER_CORES=`nproc`" >> $env
echo "export SPARK_WORKER_MEMORY=`$(( $SPARK_WORKER_CORES * $SLURM_MEM_PER_CPU ))M`" >> $env
echo "export SPARK_WORKER_OPTS='"$WORKER_OPTS"'" >> $env

scontrol show hostname $SLURM_JOB_NODELIST > $SPARK_HOME/conf/slaves

conf=$SPARK_CONF_DIR/spark-defaults.conf
echo "spark.default.parallelism" $(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS ))> $conf
echo "spark.submit.deployMode" client >> $conf
echo "spark.master" spark://`hostname`:7077 >> $conf
echo "spark.executor.cores" $NUM_EXECUTOR_CORES >> $conf
echo "spark.executor.memory" $((( $SLURM_CPUS_PER_TASK * $SLURM_MEM_PER_CPU / $GPU_PER_NODE )))M >> $conf
