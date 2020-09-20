name := "Customer Retention Strategy"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.49"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.12"
libraryDependencies += "org.apache.clerezza.ext" % "org.json.simple" % "0.4"