name := "scala-stuff"
organization := "io.moorhead"
version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "com.databricks" %% "spark-xml" % "0.8.0",
  "com.softwaremill.sttp.client" %% "core" % "2.0.0-RC6",
  "com.lihaoyi" %% "upickle" % "0.7.1"
)

mainClass in Compile := Some("io.moorhead.github.graphql.ingestion.GithubPomIngestion")
