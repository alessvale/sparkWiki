
## Spark application

if __name__ == "__main__":

    from time import sleep

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import expr, split, col, window
    from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, BooleanType

    spark = SparkSession.builder.appName("WikiStreamApp").getOrCreate()

    ## Use Kafka as a source

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
        .option("subscribe", "Wikipedia") \
        .load()

     
    lines = df.selectExpr("CAST(value AS STRING)")

    ## Extract the values from input string and cast the timestamps

    lines_proc = lines \
                .withColumn("User", split(col("value"),",").getItem(0)) \
                .withColumn("Title", split(col("value"),",").getItem(1)) \
                .withColumn("Bot", split(col("value"), ",").getItem(2)) \
                .withColumn("Timestamp", split(col("value"),",").getItem(3)) \
                .drop("value")


    data = lines_proc.select(expr("SUBSTRING(User, 6, 30)").alias("User"), expr("SUBSTRING(Title, 7, 30)").alias("Title"), expr("TRIM(SUBSTRING(Bot, 5, 10))").alias("Bot"), expr("TRIM(SUBSTRING(Timestamp, 11, 30))").cast(TimestampType()).alias("Timestamp"))

    data.printSchema()

    ## Use console as a sink where("LOWER(Bot) LIKE 'true'")

    activityCount = data.groupBy("Bot").count()

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    query = activityCount.writeStream \
             .queryName("Counting") \
             .format("memory") \
             .outputMode('complete') \
             .start()
             

    for i in range(0, 40):

        spark.sql("SELECT * FROM Counting").show()
        sleep(5)
    
    ##query.awaitTermination()


