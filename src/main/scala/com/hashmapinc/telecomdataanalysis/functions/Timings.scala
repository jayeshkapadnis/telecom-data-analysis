package com.hashmapinc.telecomdataanalysis.functions

import cats.Monoid
import cats.data.WriterT
import monix.eval.Task

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.higherKinds

case class Timings(data: Map[String, FiniteDuration])

object Timings{

  type ErrorOr[A] = Either[Throwable, Task[A]]

  type TimedTask[A] = WriterT[Task, Timings, A]

  implicit val timingsMonoid: Monoid[Timings] = new Monoid[Timings] {
    def empty: Timings = Timings(Map.empty)
    def combine(x: Timings, y: Timings): Timings = Timings(x.data ++ y.data)
  }

  implicit class TaskOps[A](task: Task[A]) {
    def timed(key: String): TimedTask[A] =
      WriterT {
        for {
          startTime <- Task.eval(System.currentTimeMillis().millis)
          result <- task
          endTime <- Task.eval(System.currentTimeMillis().millis)
        } yield (Timings(Map(key -> (endTime - startTime))), result)
      }

    def untimed: TimedTask[A] =
      WriterT(task.map((Monoid[Timings].empty, _)))
  }

  implicit class TimedTaskOps[A](task: TimedTask[A]) {
    def logTimings(heading: String): Task[A] =
      for {
        resultAndLog <- task.run
        (log, result) = resultAndLog
        _ <- Task.eval {
          println {
            List(
              s"$heading:",
              log.data
                .map {
                  case (entry, duration) =>
                    s"\t$entry: ${duration.toMillis.toString}ms"
                }
                .toList
                .mkString("\n"),
              s"\tTotal: ${log.data.values.map(_.toMillis).sum}ms"
            ).mkString("\n")
          }
        }
      } yield result
  }

}
