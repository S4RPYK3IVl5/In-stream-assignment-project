name := "In-stream-assignment-project"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "com.redislabs" %% "spark-redis" % "2.4.2"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.30"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.jfarcand" % "wcs" % "1.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.13.3"
