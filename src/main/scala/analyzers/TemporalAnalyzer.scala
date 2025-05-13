package com.ecommerceanalysis.analyzers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TemporalAnalyzer(spark: SparkSession) {

  /**
   * Analyse les variations temporelles de l'activité
   */
  def analyze(data: DataFrame): Map[String, DataFrame] = {
    // Sessions par heure
    println("\n=== Sessions par heure ===")
    val sessionsByHour = data.groupBy("hour")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases"),
        round(avg("purchase_amount"), 2).as("avg_purchase_amount")
      )
      .orderBy("hour")

    sessionsByHour.show(24)

    // Sessions par jour de la semaine
    println("\n=== Sessions par jour de la semaine ===")
    val sessionsByDayOfWeek = data.groupBy("day_of_week")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases"),
        round(avg("purchase_amount"), 2).as("avg_purchase_amount")
      )

    sessionsByDayOfWeek.show()

    // Sessions par mois
    println("\n=== Évolution mensuelle ===")
    val sessionsByMonth = data.groupBy("year", "month")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases"),
        round(sum("purchase_amount"), 2).as("total_sales"),
        round(avg("purchase_amount"), 2).as("avg_purchase_amount")
      )
      .orderBy("year", "month")

    sessionsByMonth.show()

    // Heures de pointe par jour de la semaine
    println("\n=== Heures de pointe par jour de la semaine ===")
    val peakHoursByDay = data.groupBy("day_of_week", "hour")
      .count()
      .orderBy(desc("count"))

    // Trouver l'heure de pointe pour chaque jour
    val dayWindow = org.apache.spark.sql.expressions.Window.partitionBy("day_of_week").orderBy(desc("count"))
    val peakHours = peakHoursByDay
      .withColumn("rank", row_number().over(dayWindow))
      .filter(col("rank") === 1)
      .select("day_of_week", "hour", "count")
      .orderBy("day_of_week")

    peakHours.show()

    Map(
      "sessionsByHour" -> sessionsByHour,
      "sessionsByDayOfWeek" -> sessionsByDayOfWeek,
      "sessionsByMonth" -> sessionsByMonth,
      "peakHours" -> peakHours
    )
  }

  /**
   * Analyse l'évolution des ventes sur la période étudiée
   */
  def analyzeSalesTrends(data: DataFrame): DataFrame = {
    // Agrégation des ventes par jour
    val dailySales = data
      .filter(col("has_purchased") === 1)
      .groupBy("date")
      .agg(
        sum("purchase_amount").as("total_sales"),
        count("*").as("order_count"),
        avg("purchase_amount").as("avg_order_value")
      )
      .orderBy("date")

    // Calcul de la moyenne mobile sur 7 jours
    val windowSpec = org.apache.spark.sql.expressions.Window
      .orderBy("date")
      .rowsBetween(-6, 0)

    val salesTrend = dailySales
      .withColumn("sales_7day_avg", avg("total_sales").over(windowSpec))
      .withColumn("orders_7day_avg", avg("order_count").over(windowSpec))

    println("\n=== Tendance des ventes (moyenne mobile sur 7 jours) ===")
    salesTrend.show(10)

    salesTrend
  }
}