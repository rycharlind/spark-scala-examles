ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.inndevers"

bspEnabled := false

val sparkVersion = "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "SparkScalaExamples",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test
  )
