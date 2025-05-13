package com.ecommerceanalysis.processors

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataCleaner(spark: SparkSession) {

  /**
   * Nettoie les données en traitant les valeurs nulles et aberrantes
   */
  def cleanData(data: DataFrame): DataFrame = {
    import spark.implicits._

    // 1. Affichage des statistiques sur les valeurs nulles
    val nullCounts = data.columns.map(c => (c, data.filter(col(c).isNull || col(c) === "").count()))
    println("Valeurs nulles par colonne:")
    nullCounts.foreach(println)

    // 2. Filtrage des valeurs négatives ou aberrantes dans les champs numériques
    val cleanedData = data
      .filter(col("session_duration").isNotNull && col("session_duration") >= 0)
      .filter(col("pages_viewed").isNotNull && col("pages_viewed") >= 0)

    // 3. Remplacement des valeurs nulles
    val avgReviewScore = data.select(avg("review_score")).first()(0).asInstanceOf[Double]

    val filledData = cleanedData
      .withColumn("purchase_amount", when(col("purchase_amount").isNull, 0.0).otherwise(col("purchase_amount")))
      .withColumn("review_score", when(col("review_score").isNull, avgReviewScore).otherwise(col("review_score")))

    // 4. Vérification des résultats
    println(s"Nombre de lignes avant nettoyage: ${data.count()}")
    println(s"Nombre de lignes après nettoyage: ${filledData.count()}")

    filledData
  }

  /**
   * Détecte et traite les valeurs aberrantes avec une approche statistique
   */
  def handleOutliers(data: DataFrame, column: String): DataFrame = {
    // Calcul des statistiques pour détecter les outliers (méthode IQR)
    val quantiles = data.stat.approxQuantile(column, Array(0.25, 0.75), 0.05)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr

    // Filtrage ou capping des outliers
    data.withColumn(
      column,
      when(col(column) > upperBound, upperBound)
        .when(col(column) < lowerBound, lowerBound)
        .otherwise(col(column))
    )
  }
}