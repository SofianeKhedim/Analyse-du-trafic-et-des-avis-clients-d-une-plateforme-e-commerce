package com.ecommerceanalysis.utils

import org.apache.spark.sql.SparkSession

object SparkSessionWrapper {
  // Une seule instance globale de SparkSession
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("E-Commerce Data Analysis")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.log.level", "OFF")
      .getOrCreate()
  }
}

// Trait qui donne accès à la SparkSession partagée
trait SparkSessionWrapper {
  val spark: SparkSession = SparkSessionWrapper.spark
}