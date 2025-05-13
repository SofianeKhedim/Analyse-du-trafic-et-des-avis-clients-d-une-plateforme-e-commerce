package com.ecommerceanalysis.analyzers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ReviewAnalyzer(spark: SparkSession) {

  /**
   * Analyse les avis clients
   */
  def analyze(data: DataFrame): Map[String, DataFrame] = {
    // Distribution des scores d'avis
    println("\n=== Distribution des scores d'avis ===")
    val reviewDistribution = data
      .filter(col("review_score").isNotNull)
      .groupBy("review_score")
      .count()
      .orderBy("review_score")

    reviewDistribution.show()

    // Répartition des catégories d'avis par catégorie de produit
    println("\n=== Répartition des avis par catégorie de produit ===")
    val reviewsByCategory = data
      .filter(col("review_score").isNotNull)
      .groupBy("product_category", "review_category")
      .count()
      .orderBy("product_category", "review_category")

    reviewsByCategory.show()

    // Note moyenne par pays
    println("\n=== Note moyenne par pays ===")
    val reviewsByCountry = data
      .filter(col("review_score").isNotNull)
      .groupBy("country")
      .agg(
        count("review_score").as("review_count"),
        round(avg("review_score"), 2).as("avg_review_score")
      )
      .orderBy(desc("review_count"))

    reviewsByCountry.show()

    // Analyse des thèmes récurrents dans les avis
    println("\n=== Thèmes récurrents dans les avis ===")

    // Mots fréquents dans les avis positifs
    val positiveReviews = data
      .filter(col("review_category") === "Positif" && col("review_text").isNotNull)
      .select("review_text")

    // Mots fréquents dans les avis négatifs
    val negativeReviews = data
      .filter(col("review_category") === "Négatif" && col("review_text").isNotNull)
      .select("review_text")

    // Analyser les mots fréquents
    val commonPositiveTerms = analyzeCommonTerms(positiveReviews, "review_text")
    val commonNegativeTerms = analyzeCommonTerms(negativeReviews, "review_text")

    println("Termes positifs fréquents:")
    commonPositiveTerms.show(10)

    println("Termes négatifs fréquents:")
    commonNegativeTerms.show(10)

    Map(
      "reviewDistribution" -> reviewDistribution,
      "reviewsByCategory" -> reviewsByCategory,
      "reviewsByCountry" -> reviewsByCountry,
      "commonPositiveTerms" -> commonPositiveTerms,
      "commonNegativeTerms" -> commonNegativeTerms
    )
  }

  /**
   * Analyse les termes fréquents dans une colonne de texte
   */
  private def analyzeCommonTerms(data: DataFrame, textColumn: String): DataFrame = {
    import spark.implicits._

    // Créer une fonction UDF pour traiter le texte
    val tokenizeUDF = udf((text: String) => {
      if (text == null) Seq.empty[String]
      else {
        text.toLowerCase()
          .replaceAll("[^a-zéèêëàâäôöùûüÿçœ\\s]", "")
          .split("\\s+")
          .filter(_.length > 2)
          .toSeq
      }
    })

    // Tokenize le texte
    val tokensDF = data
      .filter(col(textColumn).isNotNull)
      .withColumn("tokens", tokenizeUDF(col(textColumn)))

    // Exploser les tokens en rangées pour compter les occurrences
    val termCounts = tokensDF
      .select(explode(col("tokens")).as("term"))
      .groupBy("term")
      .count()
      .orderBy(desc("count"))

    termCounts
  }

  /**
   * Analyse la relation entre les scores d'avis et le comportement d'achat
   */
  def analyzeReviewImpact(data: DataFrame): DataFrame = {
    // Effet des avis sur les achats futurs
    val userReviewsAndPurchases = data
      .filter(col("review_score").isNotNull)
      .groupBy("user_id")
      .agg(
        avg("review_score").as("avg_user_review"),
        count("review_score").as("review_count"),
        sum("has_purchased").as("purchase_count")
      )

    // Relation entre note moyenne et nombre d'achats
    val reviewToPurchaseRelation = userReviewsAndPurchases
      .groupBy(round(col("avg_user_review")).as("rounded_review_score"))
      .agg(
        avg("purchase_count").as("avg_purchases_per_user"),
        count("*").as("user_count")
      )
      .orderBy("rounded_review_score")

    println("\n=== Impact des avis sur le comportement d'achat ===")
    reviewToPurchaseRelation.show()

    reviewToPurchaseRelation
  }
}