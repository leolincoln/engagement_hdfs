from pyspark_cassandra import CassandraSparkContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


conf = SparkConf().setAppName("NBARetrieval") \
    .set("spark.cassandra.connection.timeout_ms","20000") \
    .set("spark.cassandra.connection.host", "192.168.0.10") \
    .set("spark.cassandra.auth.username", "mdi") \
    .set("spark.cassandra.auth.password", "W2yIJw6ntl5RYC54VChe3lJoXa")


sc = CassandraSparkContext(conf=conf)
rdd = sc.cassandraTable("test", "kv")

print rdd.first()
