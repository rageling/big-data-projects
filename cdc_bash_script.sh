// SHELL SCRIPT

hadoop fs -rm -r drugdeathsbyyearout
hadoop fs -rm -r x42deathsbyyearout
hadoop fs -rm -r x44deathsbyyearout
hadoop fs -rm -r genderdrugdeathscomparisonbyyearout
hadoop fs -rm -r age2015and2010comparisonout
hadoop fs -rm -r race2015and2010comparisonout
hadoop fs -rm -r edu2015and2010comparisonout

spark-submit \
      --class sparkcdc \
      --master yarn-cluster \
      --executor-memory 1G \
      --num-executors 2 \
      --executor-cores 2 \
      /home/wang7664/Sparkcdcdrugdeaths.jar \
      /SEIS736/sparkcdc/ \
      drugdeathsbyyearout \
      x42deathsbyyearout \
      x44deathsbyyearout \
      genderdrugdeathscomparisonbyyearout \
      age2015and2010comparisonout \
      race2015and2010comparisonout \
      edu2015and2010comparisonout \
      1
hadoop fs -cat drugdeathsbyyearout/part-00000
hadoop fs -cat x42deathsbyyearout/part-00000
hadoop fs -cat x44deathsbyyearout/part-00000
hadoop fs -cat genderdrugdeathscomparisonbyyearout/part-00000
hadoop fs -cat age2015and2010comparisonout/part-00000
hadoop fs -cat race2015and2010comparisonout/part-00000
hadoop fs -cat edu2015and2010comparisonout/part-00000
