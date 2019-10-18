package com.hashmapinc.telecomdataanalysis

import java.sql.Date

import SparkSessionHelper._
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object AnalysisApp extends App{

  withLocalSparkSession{ spark =>

    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._

    def addDateTimeInformation(ds: Dataset[Data]): Dataset[Row] ={
      val d = ds
        .withColumn("activityDateTime", from_unixtime(floor($"intervalStart" / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
        .withColumn("totalActivity", $"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet").transform{ l =>
        l.withColumn("activityDate", to_date('activityDateTime, "YYYY-MM-DD"))}
      d.persist()
      d
    }

    def findMaximum(spec: WindowSpec, aggregated: DataFrame): DataFrame = {
      aggregated
        .withColumn("rank", rank().over(spec))
        .where('rank === 1)
        .drop('rank)
    }

    val classSchema = Encoders.product[Data].schema
    val data: Dataset[Data] = spark.read.format("csv")
      .option("delimiter", "\t")
      .schema(classSchema) //Need to provide schema as TSV doesn't have column labels
      .load("src/main/resources/sms-call-internet-tn-2013-11-01.txt")
      //.load("/Users/admin/Downloads/full/*.txt")
      .as[Data]

    val frame = addDateTimeInformation(data)

    //frame.map(a => ((a.gridId, new Date(a.activityDateTime.getTime), a.activityDateTime.getHours), a.totalActivity)).groupBy()

    //val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)
    val gridHourlyWindowSpec = Window.partitionBy('gridId, 'activityDate, 'hour).orderBy($"totalActivity".desc)

    //frame.map(r => (r.getAs[Int]("gridId", r.getTimestamp())))

    val d = frame.select('*, hour('activityDateTime).alias("hour"))
      .withColumn("maxTotal", first('totalActivity).over(gridHourlyWindowSpec))
      .filter('maxTotal === 'totalActivity)
      .drop("maxTotal")

    frame.filter('totalActivity === 25.00831104424966 && 'gridId === 271).show(10)

    d.show(100)

    /*val aggregated = frame.groupBy('activityDate, hour($"activityDateTime").alias("hour"))
      .agg(sum('totalActivity).alias("summedActivity"))


    findMaximum(dailyWindowSpec, aggregated).show(10)

    val gridDailyWindowSpec = Window.partitionBy('gridId, 'activityDate).orderBy('summedActivity.desc)
    val hourlyAgg = frame.groupBy('gridId, 'activityDate, hour('activityDateTime).alias("hour"))
      .agg(sum($"totalActivity").alias("summedActivity"))

    findMaximum(gridDailyWindowSpec, hourlyAgg).show(10)

    hourlyAgg
      .withColumn("rank", rank().over(dailyWindowSpec)).show(100)


    frame.groupBy('gridId, 'activityDate)
      .agg(avg('totalActivity).alias("averageActivity")).show(100)


    frame.groupBy('gridId, window('activityDate, "7 days"))
      .agg(avg('totalActivity).alias("averageActivity")).show(100)

    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("sum(sms)", "sum(call)", "sum(internet)"))
      .setOutputCols(Array("smsCategory", "callCategory", "internetCategory"))
      .setNumBucketsArray(Array(3, 3, 3))

    val countryDf = frame.na.fill(0.0)
      .select('countryCode, 'smsIn + 'smsOut alias "sms", 'callIn + 'callOut alias "call", 'internet)
      .groupBy('countryCode).sum()
    discretizer.fit(countryDf)
      .transform(countryDf).select('internetCategory).distinct()
      .show(10)*/

    /*frame.where('gridId === 388 && 'activityDate === "2013-11-02")
      .groupBy('activityDate, hour('activityDateTime).alias("hour"))
      .sum("totalActivity")
      .withColumn("rank", rank().over(Window.partitionBy('activityDate).orderBy($"sum(totalActivity)".desc)))
      .show(50)*/
  }
}