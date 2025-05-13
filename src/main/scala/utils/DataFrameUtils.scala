package com.ecommerceanalysis.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.col


object DataFrameUtils {

  /**
   * Affiche un résumé détaillé d'un DataFrame
   */
  def summarizeDataFrame(df: DataFrame, name: String = "DataFrame"): Unit = {
    println(s"\n=== Résumé de $name ===")
    println(s"Nombre de lignes: ${df.count()}")
    println(s"Nombre de colonnes: ${df.columns.length}")
    println("Schéma:")
    df.printSchema()
    println("Aperçu des données:")
    df.show(5, truncate = false)
  }

  /**
   * Optimise un DataFrame pour les performances
   */
  def optimizeDataFrame(df: DataFrame, cachingLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    // Repartitionnement optimal basé sur la taille des données
    val optimalPartitions = Math.min(Math.max(df.rdd.getNumPartitions, 8), 16)
    val repartitionedDF = df.repartition(optimalPartitions)

    // Mise en cache avec le niveau spécifié
    repartitionedDF.persist(cachingLevel)

    // Action pour déclencher la mise en cache
    repartitionedDF.count()

    repartitionedDF
  }

  /**
   * Analyse la qualité des données d'un DataFrame
   */
  def analyzeDataQuality(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calcul des stats de qualité pour chaque colonne
    val columns = df.columns
    val stats = columns.map { colName =>
      val colType = df.schema(colName).dataType.typeName

      val totalCount = df.count()
//      val nullCount = df.filter(col(colName).isNull || (colType == "string" && col(colName) === "")).count()
      val nullCondition = if (colType == "string") {
        col(colName).isNull || col(colName) === ""
      } else {
        col(colName).isNull
      }
      val nullCount = df.filter(nullCondition).count()
      val nullPercent = (nullCount.toDouble / totalCount) * 100

      val distinctCount = df.select(colName).distinct().count()
      val distinctPercent = (distinctCount.toDouble / totalCount) * 100

      // Statistiques spécifiques pour les colonnes numériques
      val resultTuple: (String, String, String, String) = if (colType == "integer" || colType == "double" || colType == "long") {
        val stats = df.select(
          org.apache.spark.sql.functions.min(col(colName)).as("min"),
          org.apache.spark.sql.functions.max(col(colName)).as("max"),
          org.apache.spark.sql.functions.avg(col(colName)).as("mean"),
          org.apache.spark.sql.functions.stddev(col(colName)).as("stdDev")
        ).first()

        (
          if (stats.isNullAt(0)) "N/A" else stats.get(0).toString,
          if (stats.isNullAt(1)) "N/A" else stats.get(1).toString,
          if (stats.isNullAt(2)) "N/A" else f"${stats.getDouble(2)}%.2f",
          if (stats.isNullAt(3)) "N/A" else f"${stats.getDouble(3)}%.2f"
        )
      } else {
        ("N/A", "N/A", "N/A", "N/A")
      }

      // Extraction des valeurs
      val min = resultTuple._1
      val max = resultTuple._2
      val mean = resultTuple._3
      val stdDev = resultTuple._4



      // Détection des valeurs aberrantes pour les colonnes numériques
      val outlierCount = if (colType == "integer" || colType == "double" || colType == "long") {
        val q1q3 = df.stat.approxQuantile(colName, Array(0.25, 0.75), 0.05)
        if (q1q3(0).isNaN || q1q3(1).isNaN) {
          0L
        } else {
          val iqr = q1q3(1) - q1q3(0)
          val lowerBound = q1q3(0) - 1.5 * iqr
          val upperBound = q1q3(1) + 1.5 * iqr
          df.filter(col(colName) < lowerBound || col(colName) > upperBound).count()
        }
      } else {
        0L
      }

      val outlierPercent = (outlierCount.toDouble / totalCount) * 100

      (
        colName,
        colType,
        totalCount,
        nullCount,
        nullPercent,
        distinctCount,
        distinctPercent,
        min,
        max,
        mean,
        stdDev,
        outlierCount,
        outlierPercent
      )
    }

    // Conversion en DataFrame
    val qualityDF = stats.toSeq.toDF(
      "column_name", "data_type", "total_count", "null_count", "null_percent",
      "distinct_count", "distinct_percent", "min_value", "max_value",
      "mean", "stddev", "outlier_count", "outlier_percent"
    )

    qualityDF
  }

  /**
   * Vérifie les performances d'un DataFrame (nombre de partitions, taille, etc.)
   */
  def checkPerformance(df: DataFrame): Unit = {
    println("\n=== Performance du DataFrame ===")
    println(s"Nombre de partitions: ${df.rdd.getNumPartitions}")
    println(s"Niveau de stockage: ${df.storageLevel}")

    // Action pour mesurer le temps de comptage
    val startTime = System.nanoTime()
    val count = df.count()
    val endTime = System.nanoTime()
    val durationMs = (endTime - startTime) / 1000000

    println(s"Comptage de $count lignes effectué en $durationMs ms")

    // Vérifier si le DataFrame est mis en cache
    if (df.storageLevel == StorageLevel.NONE) {
      println("ATTENTION: Le DataFrame n'est pas mis en cache, ce qui peut affecter les performances.")
    }
  }

  /**
   * Supprime les valeurs aberrantes d'un DataFrame pour une colonne donnée
   */
  def removeOutliers(df: DataFrame, column: String): DataFrame = {
    // Calcul des quartiles et IQR
    val quantiles = df.stat.approxQuantile(column, Array(0.25, 0.75), 0.05)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1

    // Définition des limites
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr

    // Filtrage des valeurs aberrantes
    df.filter(col(column) >= lowerBound && col(column) <= upperBound)
  }

  /**
   * Convertit un DataFrame en format CSV pour l'export
   */
  def exportToCSV(df: DataFrame, path: String, header: Boolean = true, delimiter: String = ","): Unit = {
    df.coalesce(1) // Fusionner en un seul fichier
      .write
      .option("header", header)
      .option("delimiter", delimiter)
      .mode("overwrite")
      .csv(path)

    println(s"Données exportées vers: $path")
  }
}