package com.hashmapinc.telecomdataanalysis

import monix.eval.{Coeval, Task}
import org.apache.spark.sql.SparkSession

package object compositional {

  implicit class CoevalOps[A](thunk: Coeval[A]) {
    def onPool(pool: String)(implicit session: SparkSession): Task[A] =
      Coeval(session.sparkContext.setLocalProperty("spark.scheduler.pool", pool))
        .flatMap(_ => thunk)
        .doOnFinish(_ => Coeval(session.sparkContext.setLocalProperty("spark.scheduler.pool", null)))
        .task
  }
}
