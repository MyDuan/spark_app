name := "Simple Project"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  "org.scalatest" %% "scalatest" % "3.0.7"
)
