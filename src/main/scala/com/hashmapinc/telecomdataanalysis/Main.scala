package com.hashmapinc.telecomdataanalysis

import SparkSessionHelper._
import com.hashmapinc.telecomdataanalysis.compositional.SparkPipeline
import functions.SparkFunctionalApp

object Main extends App {

  withLocalSparkSession{ spark =>
    val application = new SparkPipeline(spark)
    //application.run("src/main/resources/sms-call-internet-tn-2013-11-01.txt")

    application.run("/Volumes/Jay/Data/full")

    //System.in.read
    //spark.stop()
  }

}
