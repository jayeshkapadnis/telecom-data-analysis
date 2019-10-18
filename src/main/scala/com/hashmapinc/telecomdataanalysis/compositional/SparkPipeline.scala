package com.hashmapinc.telecomdataanalysis.compositional

import cats.data._
import cats.implicits._
import com.hashmapinc.telecomdataanalysis.Data
import com.hashmapinc.telecomdataanalysis.functions.Timings._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import scala.language.higherKinds

class SparkPipeline(spark: SparkSession) {
  import SparkPipeline._

  def run(path: String): Unit ={
    implicit val session: SparkSession = spark

    input(path).flatMap{ ds =>
      for{
        _ <- job(dailyPeakActivityPerGridInHour, showAction, ds, "First Program", "daily-peak-grid")
        _ <- job(dailyPeakActivityInHour, showAction, ds, "Second Program", "daily-peak-hour")
        _ <- job(rankGridsByPerDayActivity, showAction, ds, "Third Program", "rank-grid")
        _ <- job(averageActivityPerGridPerDay, showAction, ds, "Fourth program", "avg-activity")
        _ <- job(categorizeCountryByActivity, showAction, ds, "Category program", "cat-country")
      } yield ()
    }.doOnFinish(_ => Task.eval(spark.stop()))
      .runAsync.attempt.map{
      case Right(_) => println("Job Completed")
      case Left(e) => e.printStackTrace()
    }
  }

  private def input(path: String)(implicit spark: SparkSession): Task[Dataset[Row]] = {
    readCSVData(path)
      .andThen(replaceNull)
      .andThen(withDateTimeInformation)
      .run(spark)
      .logTimings("Initial setup")
  }

  private[compositional] def job(t: Transformer[Dataset, Row, Row],
                                 a: Action[Row, Unit],
                                 df: Dataset[Row],
                                 heading: String,
                                 pool: String)(implicit ss: SparkSession) ={
    t.andThen(a).run(df).logTimings(heading).coeval.onPool(pool)
  }

}

object SparkPipeline{

  type Id[X] = X
  type Loader[F[_], I] = Kleisli[TimedTask, SparkSession, F[I]]
  type Transformer[F[_], A, B] = Kleisli[TimedTask, F[A], Dataset[B]]
  type Action[I, O] = Kleisli[TimedTask, Dataset[I], Id[O]]

  case class Param[T](spark: SparkSession, ds: Dataset[T])

  def readCSVData(path: String, delimiter: String = "\t"): Loader[Dataset, Data] = Kleisli{ spark: SparkSession =>
    import spark.implicits._

    Task.eval {
      val classSchema = Encoders.product[Data].schema

      spark.read.format("csv")
        .option("delimiter", delimiter)
        .schema(classSchema)
        .load(path)
        .as[Data]
    }.memoize.timed("Create a DataFrame")
  }

  def replaceNull(implicit spark: SparkSession): Transformer[Dataset, Data, Data] = Kleisli{ d =>
    import spark.implicits._

    Task.eval{
      d.na.fill(0.0).as[Data]
    }.memoize.timed("Replace null values with 0")
  }

  def withDateTimeInformation(implicit spark: SparkSession): Transformer[Dataset, Data, Row] = Kleisli{ df =>
    import spark.implicits._

    Task.eval{
      df.withColumn("activityDateTime", from_unixtime(floor($"intervalStart" / 1000L), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
        .withColumn("totalActivity", $"smsIn" + $"smsOut" + $"callIn" + $"callOut" + $"internet").transform { l =>
        l.withColumn("activityDate", to_date('activityDateTime, "YYYY-MM-DD"))
      }.drop('intervalStart).repartition('activityDate)
        //.repartitionByRange('activityDate)
    }.memoize.timed("Add date time information")
  }

  def dailyPeakActivityInHour(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('summedActivity.desc)

    for {
      d <- aggregateByDateAndHour.run(df)
      r <- findRankedMaximum(dailyWindowSpec).run(d)
    } yield r
  }

  def dailyPeakActivityPerGridInHour(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._

    val gridHourlyWindowSpec = Window.
      partitionBy('gridId, 'activityDate, 'hour).
      orderBy('totalActivity.desc)

    Task.eval {
      df.select('gridId, 'activityDate, hour('activityDateTime).alias("hour"), 'totalActivity)
        .withColumn("maxTotal", first('totalActivity).over(gridHourlyWindowSpec))
        .filter('maxTotal === 'totalActivity)
        .drop("maxTotal")
    }.timed("Aggregate per hour by Grid id")
  }

  def rankGridsByPerDayActivity(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._

    val dailyWindowSpec = Window.partitionBy('activityDate).orderBy('totalActivity.desc)

    Task.eval {
      df.groupBy('gridId, 'activityDate)
        .agg(sum('totalActivity).alias("totalActivity"))
        .withColumn("rank", rank().over(dailyWindowSpec))
    }.timed("Rank grids on daily activity")
  }

  def averageActivityPerGridPerDay(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._

    Task.eval{
      df.groupBy('gridId, 'activityDate)
        .agg(avg('totalActivity).alias("averageActivity"))
    }.timed("Average activity per grid per day")
  }

  def categorizeCountryByActivity(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    for{
      d <- aggregateByCountry.run(df)
      s <- findSplits.run(d)
      r <- bucketizeActivities(d).run(s)
    } yield r
  }

  def showAction(implicit spark: SparkSession): Action[Row, Unit] = Kleisli{ df =>
    Task.eval{
      df.show(10)
    }.timed("Show data")
  }

  def saveToFile(name: String)(implicit spark: SparkSession): Action[Row, Unit] = Kleisli{ df =>
    Task.eval{
      df.write.mode(SaveMode.Overwrite).json(name)
    }.timed("Show data")
  }

  private[compositional] def aggregateByDateAndHour(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._
    Task.eval{
      df.groupBy('activityDate, hour('activityDateTime).alias("hour"))
        .agg(sum('totalActivity).alias("summedActivity"))
    }.timed("Aggregate by Date and hour")
  }

  private[compositional] def findRankedMaximum(spec: WindowSpec)
                                              (implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._

    Task.eval{
      df.withColumn("rank", rank().over(spec))
        .where('rank === 1)
        .drop('rank)
    }.timed("Ranking by window")
  }

  private[compositional] def findSplits(implicit spark: SparkSession): Action[Row, Array[Double]] = Kleisli{ df =>
    import spark.implicits._

    Task.eval{
      val row = df.agg(max('countryActivity), min('countryActivity)).first()
      findRanges(row.getDouble(1), row.getDouble(0), 3)
    }.timed("Find splits based on total activity")
  }

  private[compositional] def findRanges(min: Double, max: Double, numOfRanges: Int): Array[Double] ={
    val step = (max - min) / numOfRanges.toDouble
    val doubles: Seq[Double] = (1 until numOfRanges) map { i =>
      min + (step * i)
    }
    (min +: doubles :+ max).toArray
  }

  private[compositional] def aggregateByCountry(implicit spark: SparkSession): Transformer[Dataset, Row, Row] = Kleisli{ df =>
    import spark.implicits._
    Task.eval{
      df.groupBy('countryCode)
        .agg(sum('totalActivity).alias("countryActivity"))
    }.timed("Aggregate by country")
  }

  private[compositional] def bucketizeActivities(df: DataFrame)
                                                (implicit spark: SparkSession): Transformer[Array, Double, Row] = Kleisli { splits =>
    import spark.implicits._

    Task.eval {
      val b = new Bucketizer()
        .setInputCol("countryActivity")
        .setOutputCol("group").setSplits(splits)

      b.transform(df).withColumn("category", when('group === lit(0), "low")
        .otherwise(when('group === lit(1), "medium").otherwise("high")))
        .drop('group)
    }.timed("Bucketize Activities by Country code")
  }
}