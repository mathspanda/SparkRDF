name := "SparkRDF"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"

libraryDependencies += "org.apache.jena" % "jena-core" % "3.0.1"
libraryDependencies += "org.apache.jena" % "jena-jdbc-core" % "3.0.1"
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.0.1"
libraryDependencies += "org.apache.jena" % "jena-jdbc-driver-mem" % "3.0.1"
libraryDependencies += "org.apache.jena" % "jena-jdbc-driver-remote" % "3.0.1"

//resolvers += "Akka Repo" at "http://repo.akka.io/releases/"