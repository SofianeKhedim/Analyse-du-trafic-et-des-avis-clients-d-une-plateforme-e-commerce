package com.ecommerceanalysis.config

import org.apache.spark.sql.SparkSession

class SparkConfig {

  /**
   * Configure la session Spark avec les paramètres optimaux
   */
  def setupSpark(spark: SparkSession): Unit = {
    // Configuration pour les performances
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.default.parallelism", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    // Configuration pour la gestion de la mémoire
//    spark.conf.set("spark.memory.fraction", "0.8")
//    spark.conf.set("spark.memory.storageFraction", "0.3")

    // Configuration des checkpoints
    spark.sparkContext.setCheckpointDir("data/checkpoints")

    // Afficher la configuration active
    println("=== Configuration Spark ===")
    println(s"Nombre de cœurs disponibles: ${spark.sparkContext.defaultParallelism}")
    println(s"Mémoire allouée au driver: ${spark.conf.get("spark.driver.memory")}")
    println(s"Partition shuffle: ${spark.conf.get("spark.sql.shuffle.partitions")}")
  }

  /**
   * Configure Spark pour une exécution en mode batch (optimisé pour les gros volumes)
   */
  def setupBatchMode(spark: SparkSession): Unit = {
    // Optimisations supplémentaires pour le traitement par lots
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10 MB
    spark.conf.set("spark.sql.broadcastTimeout", "600")
    spark.conf.set("spark.executor.heartbeatInterval", "20s")
    spark.conf.set("spark.network.timeout", "300s")

    // Compression des données intermédiaires
    spark.conf.set("spark.rdd.compress", "true")
    spark.conf.set("spark.shuffle.compress", "true")
    spark.conf.set("spark.shuffle.spill.compress", "true")

    println("Spark configuré en mode batch")
  }

  /**
   * Configure Spark pour une exécution interactive (optimisé pour la réactivité)
   */
  def setupInteractiveMode(spark: SparkSession): Unit = {
    // Optimisations pour l'interactivité
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1048576") // 1 MB
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728") // 128 MB

    println("Spark configuré en mode interactif")
  }
}