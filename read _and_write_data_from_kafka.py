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
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
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
        .option("subscribe", "event_logs_3") \
        .option("minOffsetsPerTrigger", 1) \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("maxTriggerDelay","1ms")\
        .option("kafka.group.id","spark_kafka_group")\
        .option("startingOffsets", "earliest")\
        .load()

    # printing schema
    # df.printSchema()

    '''
    The schema of dataframe created from kafka topic looks like this
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    '''



    #defining schema for the datafram
    df_schema = StructType([
    StructField("ID",StringType(),True),
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


    # selecting only value column from the json
    df = df.select(from_json(col("value").cast("String"),df_schema).alias('rt'))


    # df.printSchema()

    # for selecting from the nested column
    # df = df.select(df.rt.Id,df.rt.CompanyID)

    #for selecting columns
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



    output_path = "C:\\Users\\hp\\pyspark_udemy\\output_data\\event_data"

    # Printing on terminal
    # query = df.writeStream \
    #     .format("console")\
    #     .outputMode("append") \
    #     .start()

    # Await the termination of the query
    # query.awaitTermination()




    # #writing into csv in folder
    # query = df.writeStream \
    #     .format("csv")\
    #     .option("path",output_path)\
    #     .outputMode("append") \
    #     .option("checkpointLocation","checkpointDir")\
    #     .start()
    #
    # print("records written")
    # query.awaitTermination()

    #for inserting into kafka we need key value pair
    #to_json function to convert dataframe into json to load similarly we can use csv,avro format also and the function names for them are to_csv and to_avro
    kafkf_sink_df = df.select(col("*"))\
        .withColumn("value",to_json(struct("*"))).withColumn("key",col("CompanyID"))\
        .select("key","value")

    # kafkf_sink_df.show(truncate=False)

    # for writing in kafka topic in batch mode
    # kafkf_sink_df \
    #     .write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic","output_topic")\
    #     .save()


    # to write into kafka sink in streaming mode
    # processing time is for controlling each triggering time. default 0 means it will try to complete query as soon as possible.continuous is also one option.
    # checkpointing will store inforation about upto where the records are stored and when job is restarted it will start consuming from there only.
    query = kafkf_sink_df.writeStream \
        .format("kafka")\
        .outputMode("append") \
        .option("checkpointLocation","checkpointDir") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .trigger(processingTime ='1 seconds')\
        .option("topic","output_topic")\
        .start()

    # print("records written")
    query.awaitTermination()
