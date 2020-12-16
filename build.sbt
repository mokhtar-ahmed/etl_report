import sbt.Keys.libraryDependencies

name := "etl-marketing-reports"

version := "0.1"

scalaVersion := "2.11.12"

val versions = new {
  val sparkVersion = "2.4.6"
  val jts = "1.15.1"
  val scalaTestVersion = "3.0.5"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.sparkVersion,
  "org.apache.spark" %% "spark-sql" % versions.sparkVersion,
  "org.apache.spark" %% "spark-hive" % versions.sparkVersion,
  "org.scalatest" %% "scalatest" % versions.scalaTestVersion % Test
)