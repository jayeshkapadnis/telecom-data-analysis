package com.hashmapinc.telecomdataanalysis.functions

import cats.data.Kleisli
import com.hashmapinc.telecomdataanalysis.Data
import com.hashmapinc.telecomdataanalysis.functions.Timings._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

class SparkFunctionalApp(spark: SparkSession) {

  //spark.conf.set("spark.scheduler.policy", "FAIR")

  import SparkFunctionalApp._

  type Job = Kleisli[TimedTask, SparkSession, Unit]

  def run(path: String): Unit ={

    /*val p = for {
      a <- program(path)(dailyPeakActivityInHour(_) andThen saveResult("src/main/resources/output/dailyPeakActivityInHour.json")).logTimings("First")
      b <- program(path)(dailyPeakActivityPerGridInHour(_) andThen saveResult("src/main/resources/output/dailyPeakActivityInHourByGrid.json")).logTimings("Second")
    } yield (a, b)

    p.runAsync*/
    (for {
      d <- readCSVData(path)
      n <- replaceNull(d)
      t <- addDateTimeInfo(n)
      _ <- dailyPeakActivityInHour(t) andThen saveResult("src/main/resources/output/dailyPeakActivityInHour")
      _ <- dailyPeakActivityPerGridInHour(t) andThen saveResult("src/main/resources/output/dailyPeakActivityInHourByGrid")
      _ <- rankGridsByPerDayActivity(t) andThen saveResult("src/main/resources/output/rankGridsByPerDayActivity")
    } yield ()).run(spark).logTimings("Program").runAsync

  }

  private[functions] def program(input: String)(f: DataFrame => Job): TimedTask[Unit] ={
    (for {
      d <- readCSVData(input)
      n <- replaceNull(d)
      t <- addDateTimeInfo(n)
      a <- f(t)
    } yield a).run(spark)
  }

}

object SparkFunctionalApp{

  type Transformation[T] = Kleisli[TimedTask, SparkSession, Dataset[T]]
  type Action = Kleisli[TimedTask, DataFrame, Unit]

  def readCSVData(path: String, delimiter: String = "\t"): Transformation[Data] = Kleisli{ spark: SparkSession =>
    import spark.implicits._

    val classSchema = Encoders.product[Data].schema

    Task.eval{
      spark.read.format("csv")
        .option("delimiter", delimiter)
        .schema(classSchema)
        .load(path)
        .as[Data]
    }.memoize.timed("Create a DataFrame")

  }

  def addDateTimeInfo(ds: Dataset[Data]): Transformation[Row] = Kleisli{ spark: SparkSession =>
    import spark.implicits._

    Task.eval{
      ds.withColumn("activityDateTime", from_unixtime(floor($"intervalStart" / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
        .withColumn("totalActivity", $"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet").transform{ l =>
        l.withColumn("activityDate", to_date('activityDateTime, "YYYY-MM-DD"))}
        .drop('intervalStart)
    }.memoize.timed("Add date time information")
  }

  def replaceNull(ds: Dataset[Data]): Transformation[Data] = Kleisli{ spark =>
    import spark.implicits._

    Task.eval{
      ds.na.fill(0.0)
        .as[Data]
    }.memoize.timed("Replace null values with 0")
  }


  def dailyPeakActivityInHour(df: DataFrame): Transformation[Row] = Kleisli{ spark: SparkSession =>
    import spark.implicits._

    val aggregated = Task.eval {
      df.groupBy('activityDate, hour('activityDateTime).alias("hour"))
        .agg(sum('totalActivity).alias("summedActivity"))
    }

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)

    (for{
      d <- aggregated
      r <- findRankedMaximum(dailyWindowSpec, d)(spark)
    } yield r).timed("Aggregate per hour")
  }

  def dailyPeakActivityPerGridInHour(df: DataFrame): Transformation[Row] = Kleisli{ spark: SparkSession =>
    import spark.implicits._

    implicit val session: SparkSession = spark

    val gridHourlyWindowSpec = Window.partitionBy('gridId, 'activityDate, 'hour).orderBy('totalActivity.desc)

    Task.eval {
      df.select('gridId, 'activityDate, hour('activityDateTime).alias("hour"), 'totalActivity)
        .withColumn("maxTotal", first('totalActivity).over(gridHourlyWindowSpec))
        .filter('maxTotal === 'totalActivity)
        .drop("maxTotal")
    }.timed("Aggregate per hour by Grid id")

    /*(for {
      d <- aggregateByGridDateHour
      r <- findRankedMaximum(gridDailyWindowSpec, d(df))
    } yield r).timed("Aggregate per hour by Grid id")*/
  }

  def rankGridsByPerDayActivity(df: DataFrame): Transformation[Row] = Kleisli{ spark =>
    import spark.implicits._

    implicit val session: SparkSession = spark

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('totalActivity.desc)

    Task.eval {
      df.groupBy('gridId, 'activityDate)
        .agg(sum('totalActivity).alias("totalActivity"))
        .withColumn("rank", rank().over(dailyWindowSpec))
    }.timed("Rank grids on daily activity")
  }

  def saveResult(output: String): Action = Kleisli{ df =>
    Task.eval {
      df.coalesce(1).write.mode(SaveMode.Overwrite).json(output)
    }.timed("Write to JSON")
  }

  private def aggregateByGridDateHour(implicit spark: SparkSession): Task[DataFrame => DataFrame] = {
    import spark.implicits._

    Task.eval { d: DataFrame =>
      d.groupBy('gridId, 'activityDate, hour('activityDateTime).alias("hour"))
        .agg(sum($"totalActivity").alias("summedActivity"))
    }
  }

  private def findRankedMaximum(spec: WindowSpec, aggregated: DataFrame)(implicit spark: SparkSession): Task[DataFrame] = {
    import spark.implicits._

    Task.eval{
      aggregated
        .withColumn("rank", rank().over(spec))
        .where('rank === 1)
        .drop('rank)
    }
  }
}


