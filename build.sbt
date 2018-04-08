name         := "SparkMe Project"
version      := "1.0"
organization := "kg.baaber"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
	 "org.apache.spark" %% "spark-core" % "2.2.0",
	 "org.apache.spark" %% "spark-sql" % "2.2.0"
) 

resolvers += Resolver.mavenLocal
