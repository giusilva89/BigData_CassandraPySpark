from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split(',')

    return Row(date = (fields[0]),
           tweet_id = int(fields[1]),
           text = (fields[2]),
           reply_count = int(fields[3]),
           retweet_count = int(fields[4]),
           like_count = int(fields[5]),
           quote_count = int(fields[6]))


if __name__ == "__main__":

           spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate

           usersDataset = park.read.csv("hdfs://user/maria_dev/bdsp/f1_tweets.csv",header=True)

           usersDataset.write\
           .format("org.apache.sql.cassandra")\
           .mode('append')\
           .option(table="formula1", keyspace="tweets")\
           .save()

           readUsers = spark.read\
           .format("org.apache.sql.cassandra")\
           .options(table="formula1", keyspace="tweets")\
           .load()

           readUsers.createOrReplaceTempView("formula1")

           sqlDF = spark.sql("SELECT * FROM formula1 LIMIT 10")
           sqlDF.show()

           spark.stop()

