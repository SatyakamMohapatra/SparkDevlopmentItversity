name := "DepartmentWiseCount"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided"
