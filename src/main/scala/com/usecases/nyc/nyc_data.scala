package com.usecases.nyc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import collection.JavaConverters._


object nyc_data {


  val inputFile = args(0)
  //  val inputFile = "/user/lokesh/test_csv_all/"
   // val checkpt = inputFile + "check"
  val checkpt = inputFile + "check"

  val sparkContext = spark.sparkContext

  val spark = SparkSession.builder
    .appName("NYC data")
    .getOrCreate()

  val inputSchema = StructType(
    Array(
      StructField("VendorID", StringType, true),
      StructField("tpep_pickup_datetime", StringType, true),
      StructField("tpep_dropoff_datetime", StringType, true),
      StructField("passenger_count", StringType, true),
      StructField("trip_distance", StringType, true),
      StructField("RatecodeID", StringType, true),
      StructField("store_and_fwd_flag", StringType, true),
      StructField("PULocationID", StringType, true),
      StructField("DOLocationID", StringType, true),
      StructField("payment_type", StringType, true),
      StructField("fare_amount", StringType, true),
      StructField("extra", StringType, true),
      StructField("mta_tax", StringType, true),
      StructField("tip_amount", StringType, true),
      StructField("tolls_amount", StringType, true),
      StructField("improvement_surcharge", StringType, true),
      StructField("total_amount", StringType, true))
  )

  val streamingDataFrame = spark.readStream.schema(inputSchema).csv(inputFile)
  //  val streamingDataFrame = spark.readStream.schema(inputSchema).csv("/home/lokesh/nyc_data/")

  streamingDataFrame.selectExpr("CAST(PULocationID AS STRING) AS key", "to_json(struct(*)) AS value").writeStream.format("kafka").option("topic", "svc-stream-2").option("kafka.bootstrap.servers", "master1.valhalla.phdata.io:9093,master2.valhalla.phdata.io:9093,master3.valhalla.phdata.io:9093").option("checkpointLocation", checkpt).start()


  val df = spark.readStream.format("kafka").option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer").option("security.protocol","SASL_SSL").option("sasl.kerberos.service.name","kafka").option("ssl.truststore.location","/opt/cloudera/security/pki/jks/jssecacerts-java-1.8.jks").option("ssl.truststore.password","changeit").option("kafka.bootstrap.servers", "master1.valhalla.phdata.io:9093,master2.valhalla.phdata.io:9093,master3.valhalla.phdata.io:9093").option("subscribe", "svc-stream-2").option("startingOffsets", "earliest").load()

  val interval=df.select(col("value").cast("string")) .alias("csv").select("csv.*")

  val interval2=interval.selectExpr("split(value,',')[0] as VendorID","split(value,',')[1] as tpep_pickup_datetime","split(value,',')[2] as tpep_dropoff_datetime","split(value,',')[3] as passenger_count","split(value,',')[4] as trip_distance","split(value,',')[5] as RatecodeID","split(value,',')[6] as store_and_fwd_flag","split(value,',')[7] as PULocationID","split(value,',')[8] as DOLocationID","split(value,',')[9] as payment_type","split(value,',')[10] as fare_amount","split(value,',')[11] as extra","split(value,',')[12] as mta_tax","split(value,',')[13] as tip_amount","split(value,',')[14] as tolls_amount","split(value,',')[15] as improvement_surcharge","split(value,',')[16] as total_amount")

  //val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)].select(from_json($"value", inputSchema).as("data"), $"timestamp").select("data.*", "timestamp")

  interval2.writeStream.format("console").option("truncate", "false").start().awaitTermination()

  val kuduContext = new KuduContext("master3.valhalla.phdata.io:7051,master2.valhalla.phdata.io:7051,master1.valhalla.phdata.io:7051", sparkContext)
  kuduContext.createTable(
    "test_spark_table", inputSchema, Seq("tpep_pickup_datetime"),
    new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List("tpep_pickup_datetime").asJava, 3))

  val kuduOptions: Map[String, String] = Map(
    "kudu.table" -> "<table_name>",
    "kudu.master" -> "<kudu_master_cluster>")

  kuduContext.insertRows(interval2, "test_spark_table")

  spark.read.options(kuduOptions).kudu.show


  // Check for the existence of a Kudu table
  //kuduContext.tableExists("another_table")


}
