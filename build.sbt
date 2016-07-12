import sbt.Keys._

name := "SparkStreamExamples"

version := "1.0"

scalaVersion := "2.10.6"
// version of hbase and hadoop
// these version are not match spark 1.6.0
val hbaseVersion = "0.98.4-hadoop2"
val hadoopVersion = "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll ExclusionRule(organization="javax.servlet")
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion excludeAll ExclusionRule(organization="javax.servlet")
libraryDependencies += "org.apache.hbase" % "hbase-server" % hbaseVersion excludeAll ExclusionRule(organization="org.mortbay.jetty")
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

//paypal libraries
libraryDependencies ++= Seq(
  "com.paypal.infra" % "infra-core" % "13.4.3",
  "com.paypal.risk.idi" % "common-vo" % "2.91" exclude("com.paypal.infra", "infra")
)


// Use local repositories by default
resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
  // For Typesafe goodies, if not available through maven
  // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  // For Spark development versions, if you don't want to build spark yourself
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "PayPal Nexus releases" at "http://nexus.paypal.com/nexus/content/repositories/releases",
  "PayPal Nexus snapshots" at "http://nexus.paypal.com/nexus/content/repositories/snapshots",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)


test in assembly := {}

assemblyJarName in assembly := s"${name.value}_assembly_${scalaVersion.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  //  case PathList("org", "spark", xs @ _*) => MergeStrategy.last
  case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
  case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "commons", "beanutils", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "commons", "collections", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "commons", "logging", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "hadoop", "yarn", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", "common", "base", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last

  case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in assembly := Some("com.paypal.risk.rds.KafkaStreamExample.KafkaStreamExampleMain")