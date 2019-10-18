package com.hashmapinc.telecomdataanalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionHelper extends Serializable{

  def withLocalSparkSession(f: SparkSession => Unit): Unit ={
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("telecom app")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.sql.shuffle.partitions", "350")
      .set("spark.executor.instances", "3")

    val session = SparkSession.builder().config(conf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    f(session)
  }

}
