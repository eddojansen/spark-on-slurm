##setenv-cpu.sh##
echo `hostname`

if [ ! -d "$SPARK_HOME/sbin" ]
then
    wget -c ${SPARK_URL} -O - | sudo tar --strip-components=1 --one-top-level=${SPARK_HOME} -xz
else
    echo "${SPARK_HOME} exists"
fi

sudo chown -R $(id -u):$(id -g) ${MOUNT}/spark

env=$SPARK_HOME/conf/spark-env.sh
echo "export SPARK_LOG_DIR=$SPARK_HOME/log" > $env
echo "export SPARK_WORKER_DIR=$SPARK_HOME/sparkworker" >> $env
echo "export SLURM_MEM_PER_CPU=$SLURM_MEM_PER_CPU" >> $env
echo 'export SPARK_WORKER_CORES=`nproc`' >> $env
echo 'export SPARK_WORKER_MEMORY=$(( $SPARK_WORKER_CORES*$SLURM_MEM_PER_CPU ))M' >> $env
echo "export SPARK_HOME=$SPARK_HOME" > ~/.bashrc
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc

scontrol show hostname $SLURM_JOB_NODELIST > $SPARK_HOME/conf/slaves

conf=$SPARK_HOME/conf/spark-defaults.conf
echo "spark.default.parallelism" $(( $SLURM_CPUS_PER_TASK * $SLURM_NTASKS ))> $conf
echo "spark.submit.deployMode" client >> $conf
echo "spark.master" spark://`hostname`:7077 >> $conf
echo "spark.executor.cores" $SLURM_CPUS_PER_TASK >> $conf
echo "spark.executor.memory" $(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))M >> $conf

