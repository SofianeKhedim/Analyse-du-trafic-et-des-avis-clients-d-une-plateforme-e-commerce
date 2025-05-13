package com.ecommerceanalysis.processors

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataTransformer(spark: SparkSession) {

  /**
   * Applique des transformations aux données pour préparer l'analyse
   */
  def transform(data: DataFrame): DataFrame = {
    import spark.implicits._

    // 1. Extraction d'informations temporelles
    val withTimeFeatures = data
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
      .withColumn("month", month(col("timestamp")))
      .withColumn("year", year(col("timestamp")))

    // 2. Catégorisation de la durée de session
    val withSessionCategory = withTimeFeatures
      .withColumn("session_category",
        when(col("session_duration") < 5, "Courte")
          .when(col("session_duration") >= 5 && col("session_duration") < 15, "Moyenne")
          .otherwise("Longue")
      )

    // 3. Indicateur d'achat (1 si achat, 0 sinon)
    val withPurchaseFlag = withSessionCategory
      .withColumn("has_purchased", when(col("purchase_amount") > 0, 1).otherwise(0))

    // 4. Catégorisation des avis
//    val withReviewCategory = withPurchaseFlag
//      .withColumn("review_category",
//        when(col("review_score") <= 2, "Négatif")
//          .when(col("review_score") == 3, "Neutre")
//          .when(col("review_score") >= 4, "Positif")
//          .otherwise("Non évalué")
//      )

    val withReviewCategory = withPurchaseFlag
      .withColumn("review_category",
        when(col("review_score").leq(2), "Négatif")
          .when(col("review_score").equalTo(3), "Neutre")
          .when(col("review_score").geq(4), "Positif")
          .otherwise("Non évalué")
      )

    // 5. Fenêtre pour numéroter les sessions par utilisateur
    val windowSpec = Window.partitionBy("user_id").orderBy("timestamp")
    val withSessionNumber = withReviewCategory
      .withColumn("session_number", row_number().over(windowSpec))

    withSessionNumber
  }

  /**
   * Génère des features pour la modélisation
   */
  def generateFeatures(data: DataFrame): DataFrame = {
    // Ajout de features avancées pour la modélisation

    // Ratio pages vues / durée session
    val withFeatures = data
      .withColumn("pages_per_minute",
        when(col("session_duration") > 0, col("pages_viewed") / col("session_duration"))
          .otherwise(0)
      )

    // Enrichissement avec le comportement passé des utilisateurs
    val userStats = data.groupBy("user_id")
      .agg(
        avg("purchase_amount").as("avg_purchase_amount"),
        sum("has_purchased").as("total_purchases"),
        count("*").as("total_sessions")
      )
      .withColumn("purchase_frequency", col("total_purchases") / col("total_sessions"))

    // Joindre avec les données principales
    val enrichedData = withFeatures.join(userStats, Seq("user_id"), "left")

    enrichedData
  }
}