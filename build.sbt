ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.15"
ThisBuild / organization := "com.ecommerceanalysis"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-analysis",
    libraryDependencies ++= Seq(

      // Spark
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.0",

      // Canvas et visualisation
//      "org.knowm.xchart" % "xchart" % "3.8.6",
//      "org.jfree" % "jfreechart" % "1.5.4",

      "org.apache.commons" % "commons-lang3" % "3.12.0",

// Bibliothèque pour manipuler les fichiers
      "com.github.pathikrit" %% "better-files" % "3.9.2",

      // JSON
      "io.circe" %% "circe-core" % "0.14.5",
      "io.circe" %% "circe-generic" % "0.14.5",
      "io.circe" %% "circe-parser" % "0.14.5",

      // Tests
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    ),

    // Options de compilation
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-language:postfixOps",
      "-language:implicitConversions"
    ),

    // Mainclass pour l'exécution
    Compile / mainClass := Some("com.ecommerceanalysis.Main"),
    assembly / mainClass := Some("com.ecommerceanalysis.Main"),

    // Configuration pour le plugin assembly
    assembly / assemblyJarName := "ecommerce-analysis.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )


//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.13.15"
//
//lazy val root = (project in file("."))
//  .settings(
//    name := "ecommerce-analysis",
//    idePackagePrefix := Some("com.ecommerceanalysis")
//  )
