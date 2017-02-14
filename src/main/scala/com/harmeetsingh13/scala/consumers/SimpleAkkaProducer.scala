package com.harmeetsingh13.scala.consumers

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.akka.{KafkaProducerActor, ProducerRecords}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationLong
import scala.util.{Failure, Success}

/**
  * Created by harmeet on 12/2/17.
  */
class SimpleAkkaProducer (config: Config, system: ActorSystem) {

  private val producerConf = KafkaProducer.
    Conf(config,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)

  val actorRef = system.actorOf(KafkaProducerActor.props(producerConf))

  def sendMessageWayOne(record: ProducerRecords[String, String]) = {
    actorRef ! record
  }

  def sendMessageWayTwo(record: ProducerRecords[String, String]) = {
    implicit val timeout = Timeout(100.seconds)
    val future = (actorRef ? record).mapTo[String]
    future onComplete  {
      case Success(data) => println(s" >>>>>>>>>>>> ${data}")
      case Failure(ex) => ex.printStackTrace()
    }
  }
}

object SimpleAkkaProducer {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("KafkaProducerActor")
    val config = ConfigFactory.defaultApplication()
    val simpleAkkaProducer = new SimpleAkkaProducer(config, system)

    val topic = config.getString("akka.topic")
    val messageOne = ProducerRecords.fromKeyValues[String, String](topic,
      Seq((Some("Topics"), "First Message")), None, None)

    simpleAkkaProducer.sendMessageWayOne(messageOne)
    simpleAkkaProducer.sendMessageWayTwo(messageOne)

    val onSuccess = (data: RecordMetadata) => println(s"Offest: ${data.offset()}, Topic: ${data.topic()}, " +
      s"Partition: ${data.partition()}, Timestamp: ${data.timestamp()} ")

    val messageTwo = ProducerRecords.fromKeyValues[String, String](topic,
      Seq((Some("Topics"), "Second Message")), Some(">>>>>>>>>>>>>>>"), None)

    simpleAkkaProducer.sendMessageWayOne(messageTwo)

    //Await.result(system.terminate(), 60.seconds)
  }
}
