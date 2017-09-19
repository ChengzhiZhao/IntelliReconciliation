name := "IntelliReconciliation"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
//  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-hive" % sparkVersion
)

libraryDependencies += "org.yaml" % "snakeyaml" % "1.18"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.9.0"
        