ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.4"

lazy val root = (project in file("."))
  .settings(
    name := "SparkWordCount"
  )
