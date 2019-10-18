package com.hashmapinc.telecomdataanalysis

import cats.Id
import cats.data.Reader
import monix.eval.Task
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import monix.execution.Scheduler.Implicits.global


class SparkReaderJob(spark: SparkSession) {
  import SparkReaderJob._

  def run(path: String) ={
    /*val d = readCSVData(path)
      .flatMap(addDateTimeInfo).run(spark)*/

    val dframe: Task[Id[DataFrame]] = Task.eval {
      readCSVData(path)
        .flatMap(addDateTimeInfo).run(spark)
    }.memoize


    val h: Task[DataFrame => Id[Unit]] = Task.eval { df: DataFrame =>
      dailyPeakActivityInHour(df)
        .andThen(saveResult("src/main/resources/output/dailyPeakActivityInHour.csv"))
        .run(spark)
    }

    val d = Task.eval { df: DataFrame =>
      dailyPeakActivityPerGridInHour(df)
        .andThen(saveResult("src/main/resources/output/dailyPeakActivityPerGridInHour.csv"))
        .run(spark)
    }


    (for {
      a <- dframe
      b <- h.map(_.apply(a))
      c <- d.map(_.apply(a))
    } yield (b, c)).runAsync
  }

}

object SparkReaderJob{

  type ReaderTask = Reader[SparkSession, Task[DataFrame => Unit]]
  type Action = Task[DataFrame => DataFrame]
  type Job = Task[DataFrame => Unit]

  def readCSVData(path: String, delimiter: String = "\t"): Reader[SparkSession, Dataset[Data]] = Reader{ spark: SparkSession =>
    import spark.implicits._

    val classSchema = Encoders.product[Data].schema

    spark.read.format("csv")
      .option("delimiter", delimiter)
      .schema(classSchema) //Need to provide schema as TSV doesn't have column labels
      .load(path)
      .as[Data]
  }

  def addDateTimeInfo(ds: Dataset[Data]): Reader[SparkSession, Dataset[Row]] = Reader{ spark: SparkSession =>
    import spark.implicits._

    val d = ds
      .withColumn("activityDateTime", from_unixtime(floor($"intervalStart" / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withColumn("totalActivity", $"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet").transform{ l =>
      l.withColumn("activityDate", to_date('activityDateTime, "YYYY-MM-DD"))}
    d.show(10)
    d
  }

  def findRankedMaximum(spec: WindowSpec, aggregated: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    aggregated
      .withColumn("rank", rank().over(spec))
      .where('rank === 1)
      .drop('rank)
  }

  def dailyPeakActivityInHour(frame: DataFrame): Reader[SparkSession, DataFrame] = Reader{ spark: SparkSession =>
    import spark.implicits._

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)

    findRankedMaximum(dailyWindowSpec, frame.groupBy('activityDate, hour($"activityDateTime").alias("hour"))
      .agg(sum('totalActivity).alias("summedActivity")))(spark)

  }

  def dailyPeakActivityPerGridInHour(frame: DataFrame): Reader[SparkSession, DataFrame] = Reader{ spark: SparkSession =>
    import spark.implicits._

    val gridDailyWindowSpec = Window.partitionBy('gridId, 'activityDate).orderBy('summedActivity.desc)

    val f = findRankedMaximum(gridDailyWindowSpec, frame.groupBy('gridId, 'activityDate, hour('activityDateTime).alias("hour"))
      .agg(sum($"totalActivity").alias("summedActivity")))(spark)
    f.show(10)
    f
  }

  def rankGridsByPerDayActivity(frame: DataFrame) = Reader{ spark: SparkSession =>
    import spark.implicits._

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)
    dailyPeakActivityPerGridInHour(frame)
      .map(df => df.withColumn("rank", rank().over(dailyWindowSpec)))
  }

  def saveResult(output: String): Reader[DataFrame, Unit] = Reader{ df: DataFrame =>
    df.write.mode(SaveMode.Overwrite).json(output)
  }

  def dailyPeakActivityInHourT: Reader[SparkSession, Action] = Reader{ spark: SparkSession =>
    import spark.implicits._

    Task.eval { df: DataFrame =>
      val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)

      findRankedMaximum(dailyWindowSpec, df.groupBy('activityDate, hour($"activityDateTime").alias("hour"))
        .agg(sum('totalActivity).alias("summedActivity")))(spark)
    }

  }

  def saveResultT(output: String): Reader[Action, Job] = Reader{ action: Action =>
    action.map{
      a => a.andThen(_.write.mode(SaveMode.Overwrite).json(output))
    }
  }

}
