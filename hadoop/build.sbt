ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

logLevel := Level.Info // Options: Error, Warn, Info, Debug

// Set main class
mainClass in assembly := Some("Main")

lazy val root = (project in file("."))
  .settings(
    name := "Initial Spark Hadoop",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.5", // Spark Core
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "mysql" % "mysql-connector-java" % "8.0.33",
      // Logback Classic Implementation
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      // Optional: Scala Logging wrapper (makes logging more Scala-friendly)
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
    )
  )

// Merge strategy to avoid conflicts
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)  => MergeStrategy.discard
  case "reference.conf"               => MergeStrategy.concat
  case x if x.endsWith(".properties") => MergeStrategy.first
  case x if x.contains("mysql")       => MergeStrategy.last
  case _                              => MergeStrategy.first
}
