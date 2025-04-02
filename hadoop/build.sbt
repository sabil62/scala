ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

logLevel := Level.Info // Options: Error, Warn, Info, Debug

lazy val root = (project in file("."))
  .settings(
    name := "Initial Spark Hadoop",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.4", // Spark Core
      "org.apache.spark" %% "spark-sql" % "3.4.4",
      "mysql" % "mysql-connector-java" % "8.0.33",
      // Logback Classic Implementation
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      // Optional: Scala Logging wrapper (makes logging more Scala-friendly)
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
    )
  )
