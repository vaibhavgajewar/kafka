import sys
from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import expr, concat, to_json, struct
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

if __name__ == "__main__":
    # creating spark session with dependencies for kafka and memory management
    spark = SparkSession \
        .builder \
        .appName("SSKafka") \
        .master("local[1]")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")\
        .config("spark.executor.memory","500m") \
        .config("spark.driver.memory", "500m")\
        .config("spark.memory.offHeap.enabled", "true")\
        .config("spark.memory.offHeap.size", "50m")\
        .config("kafka.session.timeout.ms",1)\
        .getOrCreate()

    # creating dataframe from spark kafka stream
    # read only for testing application then you can cosider this as batch job everything will be remain same
    # minoffsetpertrigger and maxoffsetpertirritger for taking records at a time in micorbatch
    # and trigger time can be defined in sink while writing in kafka output topic.
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "event_logs") \
        .option("maxTriggerDelay","1ms")\
        .option("kafka.group.id","spark_kafka_group")\
        .option("startingOffsets", "earliest")\
        .load()



    # defining schema for the datafram
    df_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("CompanyID", StringType(), True),
        StructField("Ack", StringType(), True),
        StructField("EventNumber", StringType(), True),
        StructField("EventParameter1", StringType(), True),
        StructField("EventParameter2", StringType(), True),
        StructField("EventParameter3", StringType(), True),
        StructField("RecordNumber", StringType(), True),
        StructField("SatelliteID", StringType(), True),
        StructField("SiteID", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("EventType", StringType(), True),
        StructField("EventParameter4", StringType(), True),
        StructField("FlowRateGPM", StringType(), True),
        StructField("RelatedEventID", StringType(), True),
        StructField("WeatherSourceID", StringType(), True)
    ])

    # selecting only value column from the json using schema defined
    df = df.select(from_json(col("value").cast("String"),df_schema).alias('rt'))

    output_path = "/home/vaibhavg/vaibhav_documents/pyspark_codes/pyspak_practice/output_files/parquet_files"

    # for selecting columns
    df = df.select("rt.ID",
                   "rt.CompanyID",
                   "rt.Ack",
                   "rt.EventNumber",
                   "rt.EventParameter1",
                   "rt.EventParameter2",
                   "rt.EventParameter3",
                   "rt.RecordNumber",
                   "rt.SatelliteID",
                   "rt.SiteID",
                   "rt.event_time",
                   "rt.WeatherSourceID",
                   "rt.EventType",
                   "rt.EventParameter4",
                   "rt.FlowRateGPM",
                   "rt.RelatedEventID"
                   )


    # static cource having names we can use here any database or any file where data is static
    event_df = spark.read.format('csv').option("header",'true').option('inferSchema','true')\
        .load('/home/vaibhavg/vaibhav_documents/pyspark_codes/pyspak_practice/sample_files/satelitid_info.csv')

    joined_df_with_names = df.join(event_df,df.EventNumber == event_df.EventNumber,'inner')

    # Printing on terminal
    query = joined_df_with_names.writeStream \
        .format("console")\
        .outputMode("append") \
        .trigger(processingTime='2 seconds') \
        .start()

    # Await the termination of the query
    query.awaitTermination()



