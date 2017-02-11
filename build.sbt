name := "learning-kafka"

version := "1.0"

scalaVersion := "2.12.1"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

resolvers += "confluent" at "http://packages.confluent.io/maven/"

val kafkaVersion = "0.10.1.1"

val scalaKafkaVersion = "0.10.1.2"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "io.confluent" % "kafka-avro-serializer" % "3.1.2",
  "net.cakesolutions" %% "scala-kafka-client" % scalaKafkaVersion,
  "net.cakesolutions" %% "scala-kafka-client-akka" % scalaKafkaVersion,
  "net.cakesolutions" %% "scala-kafka-client-testkit" % scalaKafkaVersion % "test"
)