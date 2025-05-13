package com.ecommerceanalysis.analyzers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class CustomerSegmentation(spark: SparkSession) {

  /**
   * Segmente les utilisateurs selon différentes méthodes
   */
  def performSegmentation(data: DataFrame): Map[String, DataFrame] = {
    // 1. Calcul des métriques par utilisateur
    val userMetrics = data.groupBy("user_id")
      .agg(
        count("*").as("total_sessions"),
        sum("has_purchased").as("total_purchases"),
        round(avg("purchase_amount"), 2).as("avg_purchase_value"),
        round(sum("purchase_amount"), 2).as("customer_lifetime_value"),
        round(avg("review_score"), 2).as("avg_rating"),
        max("timestamp").as("last_session")
      )
      .withColumn("purchase_frequency",
        when(col("total_sessions") > 0, col("total_purchases") / col("total_sessions"))
          .otherwise(0)
      )

    // 2. Segmentation basée sur le comportement d'achat
    val behaviorSegmentation = userMetrics
      .withColumn("user_segment",
        when(col("total_purchases") === 0, "Non-acheteur")
          .when(col("total_purchases") === 1, "One-time buyer")
          .when(col("total_purchases") >= 2 && col("total_purchases") < 5, "Repeat customer")
          .when(col("total_purchases") >= 5, "Loyal customer")
          .otherwise("Unknown")
      )

    // Distribution des segments
    println("\n=== Distribution des segments utilisateurs ===")
    val segmentDistribution = behaviorSegmentation.groupBy("user_segment")
      .agg(
        count("*").as("count"),
        round(avg("customer_lifetime_value"), 2).as("avg_clv"),
        round(avg("avg_rating"), 2).as("avg_rating")
      )
      .orderBy(desc("count"))

    segmentDistribution.show()

    // 3. Analyse RFM (Recency, Frequency, Monetary)
    val currentDate = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())

    val rfmAnalysis = userMetrics
      .withColumn("recency_days", datediff(lit(currentDate), col("last_session")))
      .withColumn("frequency", col("total_purchases"))
      .withColumn("monetary", col("customer_lifetime_value"))

    // Calcul des quintiles pour chaque dimension RFM
    val quantiles = 5 // Quintiles

    // Fonction pour calculer les quantiles d'une colonne
    def calculateQuantiles(df: DataFrame, column: String): Array[Double] = {
      df.stat.approxQuantile(column, (1 to (quantiles - 1)).map(_ / quantiles.toDouble).toArray, 0.05)
    }

    val recencyQuantiles = calculateQuantiles(rfmAnalysis, "recency_days")
    val frequencyQuantiles = calculateQuantiles(rfmAnalysis, "frequency")
    val monetaryQuantiles = calculateQuantiles(rfmAnalysis, "monetary")

    // Attribution des scores RFM (inversé pour recency: plus récent = meilleur score)
    val rfmScored = rfmAnalysis
      .withColumn("r_score",
        when(col("recency_days") <= recencyQuantiles(0), 5)
          .when(col("recency_days") <= recencyQuantiles(1), 4)
          .when(col("recency_days") <= recencyQuantiles(2), 3)
          .when(col("recency_days") <= recencyQuantiles(3), 2)
          .otherwise(1)
      )
      .withColumn("f_score",
        when(col("frequency") >= frequencyQuantiles(3), 5)
          .when(col("frequency") >= frequencyQuantiles(2), 4)
          .when(col("frequency") >= frequencyQuantiles(1), 3)
          .when(col("frequency") >= frequencyQuantiles(0), 2)
          .otherwise(1)
      )
      .withColumn("m_score",
        when(col("monetary") >= monetaryQuantiles(3), 5)
          .when(col("monetary") >= monetaryQuantiles(2), 4)
          .when(col("monetary") >= monetaryQuantiles(1), 3)
          .when(col("monetary") >= monetaryQuantiles(0), 2)
          .otherwise(1)
      )
      .withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))

    // Segmentation RFM finale
    val rfmSegmented = rfmScored
      .withColumn("rfm_segment",
        when(col("rfm_score") >= 13, "Champions")
          .when(col("rfm_score") >= 10, "Loyal Customers")
          .when(col("rfm_score") >= 9 && col("r_score") >= 4, "Recent Customers")
          .when(col("rfm_score") >= 9 && col("f_score") >= 4, "Frequent Customers")
          .when(col("rfm_score") >= 9 && col("m_score") >= 4, "Big Spenders")
          .when(col("rfm_score") >= 6 && col("rfm_score") < 9, "Potential Loyalists")
          .when(col("rfm_score") >= 5 && col("rfm_score") < 6 && col("r_score") < 2, "At Risk")
          .when(col("rfm_score") >= 4 && col("rfm_score") < 5, "About to Sleep")
          .when(col("rfm_score") >= 3 && col("rfm_score") < 4, "Need Attention")
          .otherwise("Lost")
      )

    // Distribution des segments RFM
    println("\n=== Distribution des segments RFM ===")
    val rfmDistribution = rfmSegmented.groupBy("rfm_segment")
      .agg(
        count("*").as("count"),
        round(avg("customer_lifetime_value"), 2).as("avg_clv")
      )
      .orderBy(desc("avg_clv"))

    rfmDistribution.show()

    Map(
      "behaviorSegmentation" -> behaviorSegmentation,
      "segmentDistribution" -> segmentDistribution,
      "rfmAnalysis" -> rfmScored,
      "rfmDistribution" -> rfmDistribution
    )
  }

  /**
   * Analyse de cohortes: suivi des utilisateurs au fil du temps
   */
  def performCohortAnalysis(data: DataFrame): DataFrame = {
    // 1. Extraction de la cohorte (première session de chaque utilisateur)
    val userFirstSession = data.groupBy("user_id")
      .agg(min("timestamp").as("first_session"))
      .withColumn("cohort_year", year(col("first_session")))
      .withColumn("cohort_month", month(col("first_session")))
      .withColumn("cohort", concat(col("cohort_year"), lit("-"), col("cohort_month")))

    // 2. Jointure avec toutes les sessions
    val cohortData = data.join(userFirstSession, Seq("user_id"))
      .withColumn("session_year", year(col("timestamp")))
      .withColumn("session_month", month(col("timestamp")))
      .withColumn("months_since_first",
        (col("session_year") - col("cohort_year")) * 12 + (col("session_month") - col("cohort_month"))
      )

    // 3. Analyse de la rétention par cohorte
    val cohortRetention = cohortData.groupBy("cohort", "months_since_first")
      .agg(
        countDistinct("user_id").as("active_users"),
        sum("has_purchased").as("purchases"),
        round(avg("purchase_amount"), 2).as("avg_purchase")
      )

    // 4. Calcul du taux de rétention
    val cohortSize = cohortData
      .filter(col("months_since_first") === 0)
      .groupBy("cohort")
      .agg(countDistinct("user_id").as("initial_users"))

    val retentionAnalysis = cohortRetention.join(cohortSize, Seq("cohort"))
      .withColumn("retention_rate", round(col("active_users") * 100 / col("initial_users"), 2))
      .orderBy("cohort", "months_since_first")

    println("\n=== Analyse de cohortes (taux de rétention) ===")
    retentionAnalysis.show()

    retentionAnalysis
  }
}