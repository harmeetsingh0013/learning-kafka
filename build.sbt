name := "learning-kafka"

version := "1.0"

scalaVersion := "2.12.1"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

resolvers += "confluent" at "http://packages.confluent.io/maven/"

val kafkaVersion = "0.10.1.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % kafkaVersion,
  "io.confluent" % "kafka-avro-serializer" % "3.1.2"
)

(stringType in avroConfig) := "String"