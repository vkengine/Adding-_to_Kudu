
name := "taxi_data"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.0",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.5.0",
  "org.apache.kafka" % "kafka-clients" % "2.0.0")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


