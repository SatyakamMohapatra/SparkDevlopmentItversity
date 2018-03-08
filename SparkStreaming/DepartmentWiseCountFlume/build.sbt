name := "DepartmentWiseCountFlume"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.11" % "1.6.3"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.6"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"