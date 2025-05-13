package com.ecommerceanalysis.analyzers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DescriptiveAnalyzer(spark: SparkSession) {

  /**
   * Effectue une analyse descriptive complète des données
   */
  def analyze(data: DataFrame): Map[String, DataFrame] = {
    val results = Map[String, DataFrame]()

    // Statistiques générales
    println("\n=== Statistiques descriptives ===")
    val stats = data.describe(
      "session_duration", "pages_viewed", "purchase_amount", "review_score"
    )
    stats.show()

    // Distribution des catégories de produits
    println("\n=== Distribution des catégories de produits ===")
    val categoryDistribution = data.groupBy("product_category")
      .agg(
        count("*").as("count"),
        round(avg("purchase_amount"), 2).as("avg_purchase"),
        round(avg("review_score"), 2).as("avg_review")
      )
      .orderBy(desc("count"))

    categoryDistribution.show()

    // Distribution par pays
    println("\n=== Distribution par pays ===")
    val countryDistribution = data.groupBy("country")
      .agg(
        count("*").as("count"),
        round(avg("purchase_amount"), 2).as("avg_purchase"),
        round(avg("review_score"), 2).as("avg_review")
      )
      .orderBy(desc("count"))

    countryDistribution.show()

    // Taux de conversion global
    println("\n=== Taux de conversion global ===")
    val conversionRate = data.agg(
      count("*").as("total_sessions"),
      sum("has_purchased").as("sessions_with_purchase"),
      round(sum("has_purchased") * 100.0 / count("*"), 2).as("conversion_rate_percent")
    )

    conversionRate.show()

    // Taux de conversion par appareil
    println("\n=== Taux de conversion par appareil ===")
    val conversionByDevice = data.groupBy("device_type")
      .agg(
        count("*").as("total_sessions"),
        sum("has_purchased").as("sessions_with_purchase"),
        round(sum("has_purchased") * 100.0 / count("*"), 2).as("conversion_rate_percent")
      )

    conversionByDevice.show()

    // Retour des résultats sous forme de Map pour utilisation ultérieure
    Map(
      "stats" -> stats,
      "categoryDistribution" -> categoryDistribution,
      "countryDistribution" -> countryDistribution,
      "conversionRate" -> conversionRate,
      "conversionByDevice" -> conversionByDevice
    )
  }

  /**
   * Analyse plus approfondie de la relation entre les variables
   */
  def analyzeCorrelations(data: DataFrame): DataFrame = {
    // Sélection des colonnes numériques pour l'analyse de corrélation
    val numericCols = Array("session_duration", "pages_viewed", "purchase_amount", "review_score", "has_purchased")

    // Calcul de la matrice de corrélation
    val correlations = numericCols.flatMap { col1 =>
      numericCols.map { col2 =>
        val correlation = data.stat.corr(col1, col2)
        (col1, col2, correlation)
      }
    }

    // Conversion en DataFrame pour une meilleure visualisation
    val correlationDF = spark.createDataFrame(correlations)
      .toDF("feature1", "feature2", "correlation")
      .orderBy(desc("correlation"))

    println("\n=== Matrice de corrélation ===")
    correlationDF.filter("feature1 != feature2").show(10)

    correlationDF
  }
}