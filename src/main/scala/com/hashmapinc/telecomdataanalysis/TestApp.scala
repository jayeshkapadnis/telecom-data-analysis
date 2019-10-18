package com.hashmapinc.telecomdataanalysis

import java.sql.{Date, Timestamp}

import frameless.TypedDataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import frameless.syntax._
import org.apache.spark.sql.expressions.Window

object TestApp extends App {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("telecom app")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  implicit val sqlContext: SQLContext = spark.sqlContext
  import spark.implicits._

  val classSchema = Encoders.product[Data].schema

  val start = System.currentTimeMillis()
  var data: Dataset[Data] = spark.read.format("csv")
    .option("delimiter", "\t")
    //.option("nullValue", "\t")
    .schema(classSchema) //Need to provide schema as TSV doesn't have column labels
    .load("src/main/resources/sms-call-internet-tn-2013-11-01.txt")
    //.load("/Users/admin/Downloads/full/*.txt")
    .as[Data]
  println(System.currentTimeMillis() - start)
  println(data.rdd.partitions.length)

  //val dsf = TypedDataset.create(data)

  //dsf.select(dsf('gridId))

  //data = data.repartition($"gridId")

  //println("size ", data.rdd.partitions.length)
  //println(data.rdd.partitioner)
  //println(data.explain(true))
  //println(data.rdd)

  val d = data
    .withColumn("activityDate", from_unixtime(floor($"intervalStart" / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withColumn("totalActivity", $"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet")


  d.persist()

 // println(d.where($"gridId" === 10623 and $"countryCode" === 355 and $"smsIn".isNotNull).show())

  val spec = Window.partitionBy('date)

  //private val column = window('activityDate, "1 hour")
  private val agg: DataFrame = d.groupBy(to_date($"activityDate", "YYYY-MM-DD").alias("date"),
    hour($"activityDate").alias("hour"))
    .agg(sum($"totalActivity").alias("hourlyTotalActivity"))
    .withColumn("maxActivity", max($"hourlyTotalActivity").over(spec))
    .filter($"maxActivity" === $"hourlyTotalActivity")
    .drop("hourlyTotalActivity")
    .withColumnRenamed("maxActivity", "hourlyTotalActivity")

  println(agg.rdd.toDebugString)
  println(agg.explain(true))
  println(agg.show(10, truncate = false))


  private val hourlyGridSpec = Window.partitionBy($"gridId", $"date").orderBy($"summedActivity".desc)
  private val hourlyGrid: DataFrame = d.groupBy($"gridId",
    to_date($"activityDate", "YYYY-MM-DD").alias("date"),
    window($"activityDate", "1 hour"))
    .agg(sum($"totalActivity").alias("summedActivity"))
    .withColumn("maxHourlyActivity", max($"summedActivity").over(hourlyGridSpec))
    .where($"maxHourlyActivity" === $"summedActivity")
      .drop($"summedActivity")
      .withColumnRenamed("maxHourlyActivity", "summedActivity")
    .select($"gridId", $"window.start", $"window.end", $"summedActivity")

  println(hourlyGrid.show(10))

  private val dailyAvg: DataFrame = d.groupBy($"gridId", dayofmonth($"activityDate").alias("date"))
    .agg(avg($"totalActivity").alias("avgActivity"))
    .select($"gridId", $"date", $"avgActivity")

  println(dailyAvg.show(10))

  /*val maxActivityHourly = f.select("*")
    .agg(max($"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet").alias("maxTotalActivity"))

  println(maxActivityHourly.show(10))*/

  //println(d.explain(true))
  //println(d.rdd.toDebugString)

  //d.printSchema()
  //d.show(10)

  //data.select($"gridId", unix_timestamp($"intervalStart" / 1000) as "start", unix_timestamp(($"intervalStart" / 1000) + 600 as "end"))

  // data.createOrReplaceTempView("telecom_data")

  //data.groupByKey(_.gridId)
  //val count: Long = data.filter($"gridId".isNull).count()

  data.printSchema()

  System.in.read
  spark.stop()

}

final case class Data(gridId: Int, intervalStart: Long, countryCode: Int,
                smsIn: Option[Double], smsOut: Option[Double], callIn: Option[Double],
                callOut: Option[Double], internet: Option[Double])

final case class AggregatedData(gridId: Int, activityDateTime: Timestamp, countryCode: Int, totalActivity: Option[Double])

