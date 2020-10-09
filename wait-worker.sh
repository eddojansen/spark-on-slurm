##wait-worker.sh##
. $SPARK_HOME/conf/spark-env.sh
num_workers=`cat $SPARK_HOME/conf/slaves|wc -l`
echo number of workers to be registered: $num_workers
master_logfile=`ls -tr ${SPARK_LOG_DIR}/*master* |tail -1`
worker_logfiles=`ls -tr ${SPARK_LOG_DIR}/*worker* |tail -$num_workers`
steptime=3
for i in {1..30}
do
  sleep $steptime
  num_reg=` grep 'registered' $worker_logfiles|wc -l`
  if [ $num_reg -eq $num_workers ]
  then
     break
  fi
done
echo registered workers after $((i * steptime)) seconds  :
for file in $worker_logfiles
do
  grep 'registered' $file
done
grep 'Starting Spark master' $master_logfile
grep 'Registering worker' $master_logfile|tail -$num_workers
