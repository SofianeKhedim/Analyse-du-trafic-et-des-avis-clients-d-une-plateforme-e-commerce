package com.ecommerceanalysis.loaders

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class DataLoader(spark: SparkSession) {

  /**
   * Charge les données CSV avec un schéma prédéfini
   */
  def loadCsvData(path: String): DataFrame = {
    // Définition du schéma
    val schema = StructType(Array(
      StructField("user_id", StringType, nullable = true),
      StructField("session_duration", IntegerType, nullable = true),
      StructField("pages_viewed", IntegerType, nullable = true),
      StructField("product_category", StringType, nullable = true),
      StructField("purchase_amount", DoubleType, nullable = true),
      StructField("review_score", DoubleType, nullable = true),
      StructField("review_text", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("device_type", StringType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("city", StringType, nullable = true)
    ))

    // Chargement des données
    spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .schema(schema)
      .csv(path)
  }

  /**
   * Sauvegarde les données au format Parquet
   */
  def saveAsParquet(data: DataFrame, path: String): Unit = {
    data.write.mode("overwrite").parquet(path)
  }
}