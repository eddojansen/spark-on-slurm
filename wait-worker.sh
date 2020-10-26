##wait-worker.sh##
num_workers=`cat $SPARK_HOME/conf/slaves|wc -l`
echo number of workers to be registered: $num_workers
for i in {1..100}
do
  sleep $steptime
  num_reg=`sudo docker logs master |tail -1 |grep -c "Registering worker"`
  if [ $num_reg -eq $num_workers ]
  then
     break
  fi
done
echo registered workers after $((i * steptime)) seconds  :
