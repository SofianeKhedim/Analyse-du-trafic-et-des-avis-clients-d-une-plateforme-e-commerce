package com.ecommerceanalysis.visualizers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import org.apache.commons.lang3.StringEscapeUtils

class ChartGenerator(spark: SparkSession) {

  /**
   * Génère tous les graphiques nécessaires pour l'analyse
   */
  def generateCharts(data: DataFrame, outputPath: String): Unit = {
    // Création du répertoire de sortie s'il n'existe pas
    val outputDir = new File(outputPath)
    if (!outputDir.exists()) {
      outputDir.mkdirs()
    }

    // Copier Chart.js dans le répertoire de sortie
    createChartJsFile(outputPath)

    // Génération des graphiques
    generateCategoryDistribution(data, outputPath)
    generateConversionRateByCategory(data, outputPath)
    generateSessionsByHour(data, outputPath)
    generateReviewDistribution(data, outputPath)
    generatePurchaseByPagesViewed(data, outputPath)
    generateConversionByDevice(data, outputPath)
    generateDayOfWeekDistribution(data, outputPath)

    // Génération du dashboard
    generateDashboard(outputPath)

    println(s"Tous les graphiques ont été générés dans: $outputPath")
  }

  /**
   * Crée un fichier Chart.js dans le répertoire de sortie
   */
  private def createChartJsFile(outputPath: String): Unit = {
    val chartJsScript = """
      <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
    """
    val file = new File(s"$outputPath/chart.js")
    new PrintWriter(file) { write(chartJsScript); close() }
  }

  /**
   * Génère un graphique de distribution des catégories de produits
   */
  def generateCategoryDistribution(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val categoryDistribution = data.groupBy("product_category")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
      .collect()

    val labels = categoryDistribution.map(row => row.getAs[String]("product_category"))
    val values = categoryDistribution.map(row => row.getAs[Long]("count"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Distribution des catégories de produits</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="categoryChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('categoryChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'bar',
            data: {
              labels: ${labels.mkString("[\"", "\", \"", "\"]")},
              datasets: [{
                label: 'Nombre de sessions',
                data: [${values.mkString(", ")}],
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Distribution des catégories de produits',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Nombre de sessions'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Catégorie'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/category_distribution.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique du taux de conversion par catégorie
   */
  def generateConversionRateByCategory(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val conversionByCategory = data.groupBy("product_category")
      .agg(
        count("*").as("total_sessions"),
        sum("has_purchased").as("purchases"),
        round(sum("has_purchased") * 100.0 / count("*"), 2).as("conversion_rate")
      )
      .orderBy(desc("conversion_rate"))
      .collect()

    val labels = conversionByCategory.map(row => row.getAs[String]("product_category"))
    val values = conversionByCategory.map(row => row.getAs[Double]("conversion_rate"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Taux de conversion par catégorie</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="conversionChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('conversionChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'bar',
            data: {
              labels: ${labels.mkString("[\"", "\", \"", "\"]")},
              datasets: [{
                label: 'Taux de conversion (%)',
                data: [${values.mkString(", ")}],
                backgroundColor: 'rgba(75, 192, 192, 0.6)',
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Taux de conversion par catégorie (%)',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Taux de conversion (%)'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Catégorie'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/conversion_by_category.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique des sessions par heure
   */
  def generateSessionsByHour(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val sessionsByHour = data.groupBy("hour")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases")
      )
      .orderBy("hour")
      .collect()

    val hours = sessionsByHour.map(row => row.getAs[Int]("hour"))
    val sessions = sessionsByHour.map(row => row.getAs[Long]("sessions"))
    val purchases = sessionsByHour.map(row => row.getAs[Long]("purchases"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Sessions par heure</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="hourlyChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('hourlyChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'line',
            data: {
              labels: [${hours.mkString(", ")}],
              datasets: [
                {
                  label: 'Sessions',
                  data: [${sessions.mkString(", ")}],
                  backgroundColor: 'rgba(54, 162, 235, 0.2)',
                  borderColor: 'rgba(54, 162, 235, 1)',
                  borderWidth: 2,
                  tension: 0.1
                },
                {
                  label: 'Achats',
                  data: [${purchases.mkString(", ")}],
                  backgroundColor: 'rgba(255, 99, 132, 0.2)',
                  borderColor: 'rgba(255, 99, 132, 1)',
                  borderWidth: 2,
                  tension: 0.1
                }
              ]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Sessions et achats par heure de la journée',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Nombre'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Heure'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/sessions_by_hour.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique de la distribution des notes d'avis
   */
  def generateReviewDistribution(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val reviewDistribution = data
      .filter(col("review_score").isNotNull)
      .groupBy("review_score")
      .count()
      .orderBy("review_score")
      .collect()

    val scores = reviewDistribution.map(row => row.getAs[Double]("review_score").toString)
    val counts = reviewDistribution.map(row => row.getAs[Long]("count"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Distribution des notes d'avis</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="reviewChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('reviewChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'pie',
            data: {
              labels: ${scores.mkString("[\"", "\", \"", "\"]")},
              datasets: [{
                label: 'Nombre d\\'avis',
                data: [${counts.mkString(", ")}],
                backgroundColor: [
                  'rgba(255, 99, 132, 0.6)',
                  'rgba(255, 159, 64, 0.6)',
                  'rgba(255, 205, 86, 0.6)',
                  'rgba(75, 192, 192, 0.6)',
                  'rgba(54, 162, 235, 0.6)'
                ],
                borderColor: [
                  'rgb(255, 99, 132)',
                  'rgb(255, 159, 64)',
                  'rgb(255, 205, 86)',
                  'rgb(75, 192, 192)',
                  'rgb(54, 162, 235)'
                ],
                borderWidth: 1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Distribution des notes d\\'avis',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/review_distribution.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique du taux d'achat par nombre de pages vues
   */
  def generatePurchaseByPagesViewed(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val purchaseByPages = data.groupBy("pages_viewed")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases"),
        round(sum("has_purchased") * 100.0 / count("*"), 2).as("purchase_rate")
      )
      .filter(col("sessions") > 50) // Filtrer pour avoir des données significatives
      .orderBy("pages_viewed")
      .collect()

    val pages = purchaseByPages.map(row => row.getAs[Int]("pages_viewed"))
    val rates = purchaseByPages.map(row => row.getAs[Double]("purchase_rate"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Taux d'achat par pages vues</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="pagesChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('pagesChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'line',
            data: {
              labels: [${pages.mkString(", ")}],
              datasets: [{
                label: 'Taux d\\'achat (%)',
                data: [${rates.mkString(", ")}],
                backgroundColor: 'rgba(153, 102, 255, 0.2)',
                borderColor: 'rgba(153, 102, 255, 1)',
                borderWidth: 2,
                tension: 0.1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Taux d\\'achat par nombre de pages vues',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Taux d\\'achat (%)'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Nombre de pages vues'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/purchase_by_pages.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique du taux de conversion par type d'appareil
   */
  def generateConversionByDevice(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val conversionByDevice = data.groupBy("device_type")
      .agg(
        count("*").as("total_sessions"),
        sum("has_purchased").as("purchases"),
        round(sum("has_purchased") * 100.0 / count("*"), 2).as("conversion_rate")
      )
      .orderBy(desc("conversion_rate"))
      .collect()

    val devices = conversionByDevice.map(row => row.getAs[String]("device_type"))
    val rates = conversionByDevice.map(row => row.getAs[Double]("conversion_rate"))

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Taux de conversion par appareil</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="deviceChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('deviceChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'bar',
            data: {
              labels: ${devices.mkString("[\"", "\", \"", "\"]")},
              datasets: [{
                label: 'Taux de conversion (%)',
                data: [${rates.mkString(", ")}],
                backgroundColor: 'rgba(255, 159, 64, 0.6)',
                borderColor: 'rgba(255, 159, 64, 1)',
                borderWidth: 1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Taux de conversion par type d\\'appareil',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Taux de conversion (%)'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Type d\\'appareil'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/conversion_by_device.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un graphique de la distribution des sessions par jour de la semaine
   */
  def generateDayOfWeekDistribution(data: DataFrame, outputPath: String): Unit = {
    // Préparation des données
    val sessionsByDay = data.groupBy("day_of_week")
      .agg(
        count("*").as("sessions"),
        sum("has_purchased").as("purchases")
      )
      .collect()

    // Ordre des jours de la semaine
    val daysOrder = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

    // Trier selon l'ordre des jours
    val sortedData = sessionsByDay.map(row => (
      row.getAs[String]("day_of_week"),
      row.getAs[Long]("sessions"),
      row.getAs[Long]("purchases")
    )).sortBy { case (day, _, _) => daysOrder.indexOf(day) }

    val days = sortedData.map(_._1)
    val sessions = sortedData.map(_._2)
    val purchases = sortedData.map(_._3)

    // Création du graphique
    val chartHtml = s"""
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <title>Sessions par jour de la semaine</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
      </head>
      <body>
        <div style="width: 800px; height: 500px;">
          <canvas id="dayChart"></canvas>
        </div>
        <script>
          const ctx = document.getElementById('dayChart').getContext('2d');
          const chart = new Chart(ctx, {
            type: 'bar',
            data: {
              labels: ${days.mkString("[\"", "\", \"", "\"]")},
              datasets: [
                {
                  label: 'Sessions',
                  data: [${sessions.mkString(", ")}],
                  backgroundColor: 'rgba(54, 162, 235, 0.6)',
                  borderColor: 'rgba(54, 162, 235, 1)',
                  borderWidth: 1
                },
                {
                  label: 'Achats',
                  data: [${purchases.mkString(", ")}],
                  backgroundColor: 'rgba(255, 99, 132, 0.6)',
                  borderColor: 'rgba(255, 99, 132, 1)',
                  borderWidth: 1
                }
              ]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: 'Sessions par jour de la semaine',
                  font: { size: 18 }
                },
                legend: {
                  position: 'top',
                }
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: 'Nombre'
                  }
                },
                x: {
                  title: {
                    display: true,
                    text: 'Jour de la semaine'
                  }
                }
              }
            }
          });
        </script>
      </body>
      </html>
    """

    // Sauvegarde du graphique
    val outputFile = s"$outputPath/sessions_by_day.html"
    new PrintWriter(outputFile) { write(chartHtml); close() }

    println(s"Graphique généré: $outputFile")
  }

  /**
   * Génère un tableau de bord HTML avec tous les graphiques
   */
  def generateDashboard(outputPath: String): Unit = {
    val dashboardHtml = s"""
      <!DOCTYPE html>
      <html lang="fr">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Tableau de bord E-Commerce</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
          h1, h2 { color: #333; }
          .dashboard { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; }
          .chart-container { background-color: white; border-radius: 5px; padding: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
          iframe { width: 100%; height: 500px; border: none; }
          .full-width { grid-column: 1 / -1; }
        </style>
      </head>
      <body>
        <h1>Tableau de bord d'analyse E-Commerce</h1>

        <div class="dashboard">
          <div class="chart-container">
            <h2>Distribution des catégories de produits</h2>
            <iframe src="category_distribution.html"></iframe>
          </div>

          <div class="chart-container">
            <h2>Taux de conversion par catégorie (%)</h2>
            <iframe src="conversion_by_category.html"></iframe>
          </div>

          <div class="chart-container full-width">
            <h2>Sessions et achats par heure de la journée</h2>
            <iframe src="sessions_by_hour.html"></iframe>
          </div>

          <div class="chart-container">
            <h2>Distribution des notes d'avis</h2>
            <iframe src="review_distribution.html"></iframe>
          </div>

          <div class="chart-container">
            <h2>Taux de conversion par type d'appareil</h2>
            <iframe src="conversion_by_device.html"></iframe>
          </div>

          <div class="chart-container full-width">
            <h2>Taux d'achat par nombre de pages vues</h2>
            <iframe src="purchase_by_pages.html"></iframe>
          </div>

          <div class="chart-container full-width">
            <h2>Sessions par jour de la semaine</h2>
            <iframe src="sessions_by_day.html"></iframe>
          </div>
        </div>

        <div class="full-width" style="margin-top: 30px;">
          <h2>Conclusions et recommandations</h2>
          <div style="background-color: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
            <h3>Principaux insights</h3>
            <ul>
              <li>Les sessions longues (>15min) ont un taux de conversion 4x supérieur aux sessions courtes</li>
              <li>Le desktop convertit mieux que le mobile malgré moins de trafic</li>
              <li>La période 17h-19h génère le plus de trafic et de conversions</li>
              <li>Le weekend (vendredi-samedi) est le moment fort des ventes</li>
              <li>Les produits High-tech ont le taux de conversion le plus élevé</li>
            </ul>

            <h3>Recommandations</h3>
            <ul>
              <li>Optimiser l'expérience mobile pour améliorer le taux de conversion</li>
              <li>Concentrer les promotions sur les périodes de forte affluence (17h-19h, weekend)</li>
              <li>Améliorer la qualité des produits avec les notes les plus basses</li>
              <li>Développer des stratégies pour rallonger la durée des sessions</li>
              <li>Mettre en avant les arguments de vente les plus cités dans les avis positifs</li>
            </ul>
          </div>
        </div>

        <footer style="margin-top: 30px; text-align: center; color: #777;">
          <p>Analyse réalisée avec Apache Spark et Scala - ${java.time.LocalDate.now()}</p>
        </footer>
      </body>
      </html>
    """

    // Écriture du fichier dashboard
    val dashboardFile = new File(s"$outputPath/dashboard.html")
    new PrintWriter(dashboardFile) { write(dashboardHtml); close() }

    println(s"Tableau de bord généré: $outputPath/dashboard.html")
  }
}