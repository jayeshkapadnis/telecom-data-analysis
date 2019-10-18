package com.hashmapinc

import monix.eval.{Coeval, Task}
import org.apache.spark.sql.SparkSession

package object telecomdataanalysis {

  implicit class CoevalOps[A](thunk: Coeval[A]) {
    def onPool(pool: String)(implicit session: SparkSession): Task[A] =
      Coeval(session.sparkContext.setLocalProperty("spark.scheduler.pool", pool))
        .flatMap(_ => thunk)
        .doOnFinish(_ => Coeval(session.sparkContext.setLocalProperty("spark.scheduler.pool", null)))
        .task
  }

}
