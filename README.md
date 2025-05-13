# Analyse de données e-commerce avec Apache Spark et Scala

## Description du projet

Ce projet utilise Apache Spark avec Scala pour analyser un jeu de données e-commerce de 550 000 lignes contenant des informations sur les sessions utilisateurs, les achats et les avis clients. L'objectif est d'extraire des insights commerciaux pertinents et de générer des visualisations interactives pour faciliter la prise de décision.

### Équipe

* Sofiane Khedim - Développement et analyse de données

## Structure du projet

```plaintext
ecommerce-analysis/
│
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── com/
│   │   │   │   └── ecommerceanalysis/
│   │   │   │       ├── Main.scala                   # Point d'entrée principal
│   │   │   │       ├── config/
│   │   │   │       │   └── SparkConfig.scala        # Configuration Spark
│   │   │   │       ├── loaders/
│   │   │   │       │   └── DataLoader.scala         # Chargement des données
│   │   │   │       ├── processors/
│   │   │   │       │   ├── DataCleaner.scala        # Nettoyage des données
│   │   │   │       │   └── DataTransformer.scala    # Transformation des données
│   │   │   │       ├── analyzers/
│   │   │   │       │   ├── DescriptiveAnalyzer.scala    # Analyses descriptives
│   │   │   │       │   ├── TemporalAnalyzer.scala       # Analyses temporelles
│   │   │   │       │   ├── CustomerSegmentation.scala   # Segmentation des clients
│   │   │   │       │   └── ReviewAnalyzer.scala         # Analyse des avis
│   │   │   │       ├── visualizers/
│   │   │   │       │   ├── ChartGenerator.scala         # Génération des graphiques
│   │   │   │       │   └── CanvasVisualizer.scala       # Visualisation avec Canvas
│   │   │   │       └── utils/
│   │   │   │           └── SparkSessionWrapper.scala    # Wrapper pour SparkSession
│   │
├── data/                            # Dossier pour les données
│   ├── input/
│   │   └── ecommerce_data.csv       # Données brutes
│   └── output/                      # Dossiers pour les résultats
│       └── visualizations/
│
├── build.sbt                        # Configuration SBT
└── project/                         # Configuration du projet
```

## Prérequis

* Java 8 ou 11
* Scala 2.13.15
* SBT 1.10.11
* Apache Spark 3.5.0

## Installation et configuration

1. Clonez ce dépôt :

```bash
git clone https://github.com/votre-utilisateur/ecommerce-analysis.git
cd ecommerce-analysis
```

2. Placez le fichier de données `ecommerce_data.csv` dans le répertoire `data/input/`.

3. Compilez le projet avec SBT :
   sbt compile

## Instructions d'exécution

### 1. Exécution complète

Pour exécuter l'analyse complète en une seule fois :

```bash
sbt run
Cette commande exécute le Main.scala qui se charge de :

Charger les données
Nettoyer et transformer les données
Effectuer les analyses
Générer les visualisations dans le dossier data/output/visualizations/
```

2. Exécution par étapes (pour le développement)
   Si vous souhaitez exécuter des parties spécifiques du code, vous pouvez utiliser le shell SBT :

```bash
# Démarrer le shell SBT
sbt

# Exécuter des classes spécifiques pour tester des fonctionnalités individuelles
runMain com.ecommerceanalysis.loaders.DataLoader
runMain com.ecommerceanalysis.processors.DataCleaner
runMain com.ecommerceanalysis.visualizers.ChartGenerator
```

3. Visualisation des résultats
   Après l'exécution, vous pouvez visualiser les résultats en ouvrant le fichier de tableau de bord HTML généré :
   data/output/visualizations/dashboard.html
   Ce tableau de bord interactif contient tous les graphiques et visualisations générés par l'analyse.

## Résumé des résultats

### Principales découvertes

* **Comportements d'achat :**

  * Les sessions longues (>15min) ont un taux de conversion 4x supérieur aux sessions courtes
  * Le desktop convertit mieux que le mobile (19.3% vs 14.8%) malgré moins de trafic

* **Analyse temporelle :**

  * Pic d'activité entre 17h et 19h
  * Le weekend (vendredi-samedi) est le moment fort des ventes

* **Analyse des produits :**

  * Les produits High-tech ont le taux de conversion le plus élevé
  * Les Livres reçoivent les meilleures notes (3.4/5)

* **Analyse géographique :**

  * Les clients suisses sont les plus satisfaits (3.4/5)
  * Les clients tunisiens attribuent les notes les plus basses (2.9/5)

## Visualisations

![Distribution des sessions par catégorie de produit](/data/output/visualizations/png/Distribution_sessions_categorie.png "")
Distribution des sessions par catégorie de produit

![Taux de conversion par catégorie de produit](/data/output/visualizations/png/Taux_conversion_categorie.png "")
Taux de conversion par catégorie de produit


![Nombre de sessions et achats par heure de la journée](/data/output/visualizations/png/Nombre_sessions_achats_heure.png "")
Nombre de sessions et achats par heure de la journée

> Pour visualiser l'ensemble des graphiques interactifs, consultez le tableau de bord complet (`dashboard.html`). 

## Difficultés rencontrées

* **Compatibilité avec Scala 2.13 :** La bibliothèque Vegas n'étant pas compatible avec Scala 2.13, nous avons dû utiliser Chart.js pour les visualisations.
* **Multiples SparkContexts :** Problème de création de plusieurs instances SparkContext résolu en utilisant un pattern singleton.
* **Valeurs manquantes :** Certaines colonnes contenaient des valeurs manquantes ou aberrantes qui ont nécessité un nettoyage minutieux.

## Axes d'amélioration

* Implémenter un modèle prédictif plus sophistiqué (Random Forest)
* Ajouter une analyse de segmentation RFM plus détaillée
* Intégrer d'autres sources de données (météo, événements) pour enrichir l'analyse
* Développer une API pour servir les prédictions en temps réel
