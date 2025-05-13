package com.ecommerceanalysis

import com.ecommerceanalysis.analyzers.{CustomerSegmentation, DescriptiveAnalyzer, ReviewAnalyzer, TemporalAnalyzer}
import com.ecommerceanalysis.config.SparkConfig
import com.ecommerceanalysis.loaders.DataLoader
import com.ecommerceanalysis.models.PurchasePrediction
import com.ecommerceanalysis.processors.{DataCleaner, DataTransformer}
import com.ecommerceanalysis.utils.SparkSessionWrapper
import com.ecommerceanalysis.visualizers.{CanvasVisualizer, ChartGenerator}
import org.apache.spark.sql.DataFrame

object Main extends App with SparkSessionWrapper {

  // Mesure du temps d'exécution
  val startTime = System.nanoTime()

  println("=== E-Commerce Data Analysis ===")

  try {
    // 1. Configuration et initialisation
    val sparkConfig = new SparkConfig()
    sparkConfig.setupSpark(spark)

    // 2. Chargement des données
    val dataPath = "data/input/ecommerce_data.csv"
    val rawData = new DataLoader(spark).loadCsvData(dataPath)
    println(s"Données chargées: ${rawData.count()} lignes")

    // 3. Nettoyage des données
    val cleaner = new DataCleaner(spark)
    val cleanedData = cleaner.cleanData(rawData)
    println(s"Données nettoyées: ${cleanedData.count()} lignes")

    // 4. Transformation des données
    val transformer = new DataTransformer(spark)
    val transformedData = transformer.transform(cleanedData)
    transformedData.cache()
    println("Données transformées et mises en cache")

//     5. Analyses
    runAnalyses(transformedData)

//    // 6. Modélisation
//    val model = new PurchasePrediction(spark)
//    val modelResults = model.buildModel(transformedData)
//    println(s"Modèle construit avec AUC: ${modelResults._1}")

    // 7. Visualisations
    createVisualizations(transformedData)

    // Mesure du temps total
    val endTime = System.nanoTime()
    val durationMs = (endTime - startTime) / 1000000
    println(s"Analyse terminée en $durationMs ms")

  } catch {
    case e: Exception =>
      println(s"Erreur lors de l'exécution: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
  }

  def runAnalyses(data: DataFrame): Unit = {
    println("\n=== Analyses ===")

    // Analyse descriptive
    val descriptiveAnalyzer = new DescriptiveAnalyzer(spark)
    val descriptiveResults = descriptiveAnalyzer.analyze(data)

    // Analyse temporelle
    val temporalAnalyzer = new TemporalAnalyzer(spark)
    val temporalResults = temporalAnalyzer.analyze(data)

    // Segmentation client
    val segmentation = new CustomerSegmentation(spark)
    val segmentationResults = segmentation.performSegmentation(data)

    // Analyse des avis
    val reviewAnalyzer = new ReviewAnalyzer(spark)
    val reviewResults = reviewAnalyzer.analyze(data)

    println("Analyses terminées avec succès")
  }

  def createVisualizations(data: DataFrame): Unit = {
    println("\n=== Création des visualisations ===")
//
//    // Génération des graphiques
//    val chartGenerator = new ChartGenerator(spark)
//    chartGenerator.generateCharts(data, "data/output/visualizations/charts")

    // Canvas pour visualisations interactives
    val canvasVisualizer = new CanvasVisualizer(spark)
    canvasVisualizer.createDashboard(data, "data/output/visualizations/dashboard")

    println("Visualisations créées avec succès")
  }
}


//package com.ecommerceanalysis
//
//
//object Main {
//  def main(args: Array[String]): Unit = {
//    println("Hello world!")
//  }
//}