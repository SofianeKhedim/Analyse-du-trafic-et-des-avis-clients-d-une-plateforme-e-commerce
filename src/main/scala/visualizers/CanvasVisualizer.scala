package com.ecommerceanalysis.visualizers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import org.apache.commons.lang3.StringEscapeUtils

class CanvasVisualizer(spark: SparkSession) {

  /**
   * Crée un dashboard HTML avec des visualisations Canvas via Chart.js
   */
  def createDashboard(data: DataFrame, outputPath: String): Unit = {
    // S'assurer que le répertoire de sortie existe
    val outputDir = new File(outputPath)
    if (!outputDir.exists()) {
      outputDir.mkdirs()
    }

    // Générer toutes les visualisations
    val chartGenerator = new ChartGenerator(spark)
    chartGenerator.generateCharts(data, outputPath)

    println(s"Dashboard créé avec succès: $outputPath/dashboard.html")
  }
}