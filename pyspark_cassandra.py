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
           quote_count = int(fields[6])

if __name__ == "__main__":

           spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connections.host", "127.0.0.1").getOrCreate


           lines = spark.sparkContext.textFile("hdfs://user/maria_dev/bdsp/f1_tweets.csv")

           users = lines.map(parseInput)

           usersDataset = spark.createDataFrame(users)

           usersDataset.write\
           .format("org.apache.sql.cassandra")\
           .mode('append')\
           .option(table="formula1", keyspace="tweets")\
           .save()

