package com.ecommerceanalysis.models

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class PurchasePrediction(spark: SparkSession) {

  /**
   * Construit un modèle de prédiction d'achat
   * @return Tuple contenant l'AUC et le modèle entraîné
   */
  def buildModel(data: DataFrame): (Double, PipelineModel) = {
    println("\n=== Construction du modèle de prédiction d'achat ===")

    // 1. Préparation des données
    val modelData = data.select(
      col("session_duration"),
      col("pages_viewed"),
      col("device_type"),
      col("product_category"),
      col("hour"),
      col("day_of_week"),
      col("has_purchased").as("label")
    )

    // 2. Configuration des transformateurs pour les variables catégorielles
    // Device type
    val deviceIndexer = new StringIndexer()
      .setInputCol("device_type")
      .setOutputCol("device_index")
      .setHandleInvalid("keep")

    val deviceEncoder = new OneHotEncoder()
      .setInputCol("device_index")
      .setOutputCol("device_vec")

    // Product category
    val categoryIndexer = new StringIndexer()
      .setInputCol("product_category")
      .setOutputCol("category_index")
      .setHandleInvalid("keep")

    val categoryEncoder = new OneHotEncoder()
      .setInputCol("category_index")
      .setOutputCol("category_vec")

    // Day of week
    val dayIndexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_index")
      .setHandleInvalid("keep")

    val dayEncoder = new OneHotEncoder()
      .setInputCol("day_index")
      .setOutputCol("day_vec")

    // 3. Assemblage des features dans un vecteur
    val assembler = new VectorAssembler()
      .setInputCols(Array("session_duration", "pages_viewed", "hour", "device_vec", "category_vec", "day_vec"))
      .setOutputCol("features")

    // 4. Modèle de régression logistique
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 5. Création d'un pipeline
    val pipeline = new Pipeline()
      .setStages(Array(
        deviceIndexer, deviceEncoder,
        categoryIndexer, categoryEncoder,
        dayIndexer, dayEncoder,
        assembler, lr
      ))

    // 6. Division des données en ensembles d'entraînement et de test
    val Array(trainingData, testData) = modelData.randomSplit(Array(0.7, 0.3), seed = 42)

    println(s"Données d'entraînement: ${trainingData.count()} lignes")
    println(s"Données de test: ${testData.count()} lignes")

    // 7. Entraînement du modèle
    val model = pipeline.fit(trainingData)

    // 8. Évaluation du modèle
    val predictions = model.transform(testData)

    // Affichage de quelques prédictions
    println("\n=== Exemples de prédictions ===")
    predictions.select("label", "prediction", "probability")
      .withColumn("purchase_probability", round(col("probability").getItem(1) * 100, 2))
      .show(10)

    // Calcul de l'AUC (Area Under ROC Curve)
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predictions)
    println(s"AUC: $auc")

    // 9. Extraction et affichage des coefficients du modèle
    val lrModel = model.stages.last.asInstanceOf[LogisticRegression]

    println("\n=== Coefficients du modèle ===")
    println(s"Intercept: ${lrModel.fitIntercept}")

    // 10. Enregistrement du modèle
    val modelPath = "data/models/purchase_prediction_model"
    model.write.overwrite().save(modelPath)
    println(s"Modèle enregistré: $modelPath")

    (auc, model)
  }

  /**
   * Optimise le modèle via une validation croisée
   */
  def buildOptimizedModel(data: DataFrame): (Double, PipelineModel) = {
    println("\n=== Construction du modèle optimisé ===")

    // Préparation des données (comme précédemment)
    val modelData = data.select(
      col("session_duration"),
      col("pages_viewed"),
      col("device_type"),
      col("product_category"),
      col("hour"),
      col("day_of_week"),
      col("has_purchased").as("label")
    )

    // Pipeline des transformateurs (comme précédemment)
    val deviceIndexer = new StringIndexer().setInputCol("device_type").setOutputCol("device_index").setHandleInvalid("keep")
    val deviceEncoder = new OneHotEncoder().setInputCol("device_index").setOutputCol("device_vec")
    val categoryIndexer = new StringIndexer().setInputCol("product_category").setOutputCol("category_index").setHandleInvalid("keep")
    val categoryEncoder = new OneHotEncoder().setInputCol("category_index").setOutputCol("category_vec")
    val dayIndexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_index").setHandleInvalid("keep")
    val dayEncoder = new OneHotEncoder().setInputCol("day_index").setOutputCol("day_vec")
    val assembler = new VectorAssembler()
      .setInputCols(Array("session_duration", "pages_viewed", "hour", "device_vec", "category_vec", "day_vec"))
      .setOutputCol("features")

    // Modèle Random Forest (alternative à la régression logistique)
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(42)

    // Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(
        deviceIndexer, deviceEncoder,
        categoryIndexer, categoryEncoder,
        dayIndexer, dayEncoder,
        assembler, rf
      ))

    // Grille de paramètres pour la validation croisée
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(10, 20, 30))
      .addGrid(rf.maxDepth, Array(5, 10, 15))
      .addGrid(rf.impurity, Array("gini", "entropy"))
      .build()

    // Évaluateur
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    // Validation croisée
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Nombre de plis pour la validation croisée
      .setSeed(42)

    // Division des données
    val Array(trainingData, testData) = modelData.randomSplit(Array(0.7, 0.3), seed = 42)

    println("Démarrage de la validation croisée (peut prendre un certain temps)...")
    val cvModel = cv.fit(trainingData)

    // Modèle avec les meilleurs paramètres
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]

    // Évaluation sur les données de test
    val predictions = bestModel.transform(testData)
    val auc = evaluator.evaluate(predictions)

    println(s"AUC du modèle optimisé: $auc")

    // Enregistrement du modèle optimisé
    val modelPath = "data/models/optimized_purchase_prediction_model"
    bestModel.write.overwrite().save(modelPath)
    println(s"Modèle optimisé enregistré: $modelPath")

    (auc, bestModel)
  }

  /**
   * Prédit la probabilité d'achat pour de nouvelles sessions
   */
  def predictPurchaseProbability(model: PipelineModel, newSessions: DataFrame): DataFrame = {
    // Application du modèle sur les nouvelles données
    val predictions = model.transform(newSessions)

    // Extraction de la probabilité d'achat et formatage
    val results = predictions.select(
        col("*"),
        round(col("probability").getItem(1) * 100, 2).as("purchase_probability"),
        col("prediction").as("predicted_purchase")
      )
      .orderBy(desc("purchase_probability"))

    results
  }
}