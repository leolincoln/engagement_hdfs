#~/spark/bin/spark-submit --class "HitchCockProcess" --master "spark://wolf.iems.northwestern.edu:7077" target/TestCassandraMaven-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ $# -eq 0 ]
    then
        echo "Usage: ./runYarn.bash fileName"
        exit
fi
#if [ ! -f $1  ]; then
#    echo "file "$1" Not found!"
#    echo "Usage: ./runYarn.bash fileName"
#    exit
#fi

export HADOOP_CONF_DIR=/etc/alternatives/hadoop-conf 
/opt/cloudera/parcels/CDH/bin/spark-submit \
--executor-cores 8 \
--executor-memory 7g \
--master yarn \
--deploy-mode client \
--num-executors 4 \
--executor-cores 8 \
--executor-memory 7g \
$1 $2 $3
