package com.hashmapinc.telecomdataanalysis

import java.sql.Timestamp
import SparkSessionHelper._
import com.hashmapinc.telecomdataanalysis.TestApp.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, SQLContext, SparkSession}

import scala.concurrent.duration.FiniteDuration

object PartitionedOnDate extends App{

  /*def transform[T](name: String, f: => Dataset[T]): Dataset[T] ={
    for{
      start <- System.currentTimeMillis().toString
      d <- f()
      _ <- name + (System.currentTimeMillis() - start.toLong)
    } yield d
  }*/

  withLocalSparkSession { spark =>
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._

    val classSchema = Encoders.product[Data].schema

    val schema = StructType(
      Array(StructField("gridId", IntegerType, nullable = false),
        StructField("activityStartEpoch", LongType, nullable = false),
        StructField("countryCode", IntegerType, nullable = false),
        StructField("smsIn", DoubleType),
        StructField("smsOut", DoubleType),
        StructField("callIn", DoubleType),
        StructField("callOut", DoubleType),
        StructField("internet", DoubleType)
      )
    )

    //println(classSchema.printTreeString())

    val start = System.currentTimeMillis()
    val data: Dataset[TData] = spark.read.format("csv")
      .option("delimiter", "\t")
      //.option("nullValue", "\t")
      .schema(schema) //Need to provide schema as TSV doesn't have column labels
      .load("src/main/resources/sms-call-internet-tn-2013-11-01.txt")
      //.load("/Users/admin/Downloads/full/*.txt")
      .withColumn("activityDateTime", from_unixtime(floor('activityStartEpoch / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .drop('activityStartEpoch)
      .repartition(200, date_format('activityDateTime, "yyyy-MM-dd"))
      .as[TData]

    println(System.currentTimeMillis() - start)
    //data.rdd.partitioner.foreach(p => println(p.getClass.getCanonicalName))
    println(data.rdd.partitions.length)
    println(data.show(10))
   // data.rdd.glom().foreach(a => println(a.length))
    System.in.read
    spark.stop()
  }

}

final case class TData(gridId: Int, activityDateTime: Timestamp, countryCode: Int,
                       smsIn: Option[Double], smsOut: Option[Double], callIn: Option[Double],
                       callOut: Option[Double], internet: Option[Double])
