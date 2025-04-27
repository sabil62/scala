ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

lazy val root = (project in file("."))
  .settings(
    name := "practice-1",
    libraryDependencies ++= Seq(
      // spark core version should be similar to your local setup
      // for more info
      // https://www.notion.so/Find-Spark-Version-1e2d539d9d3b804389e3f1aae75cfd38
      "org.apache.spark" %% "spark-core" % "3.5.5", // Spark Core
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "mysql" % "mysql-connector-java" % "8.0.33",
      "io.delta" %% "delta-core" % "2.4.0",
      "mysql" % "mysql-connector-java" % "8.0.33",
      // Logback Classic Implementation
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      // Optional: Scala Logging wrapper (makes logging more Scala-friendly)
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
    )
  )

logLevel := Level.Warn // Options: Error, Warn, Info, Debug
