#~/spark/bin/spark-submit --class "HitchCockProcess" --master "spark://wolf.iems.northwestern.edu:7077" target/TestCassandraMaven-0.0.1-SNAPSHOT-jar-with-dependencies.jar
#/opt/cloudera/parcels/CDH/bin/spark-submit \
if [ $# -eq 0 ]
    then
        echo "Usage: ./runYarn.bash fileName"
        exit
fi
spark-submit \
--deploy-mode client \
--name 'hadoop engagement' \
--executor-cores 4 \
--executor-memory 4g \
$1 
