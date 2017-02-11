package com.harmeetsingh13.scala.consumers

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Failure, Success}

/**
  * Created by harmeet on 10/2/17.
  */

class SimpleProducer(config: Config) {

  private val producer = KafkaProducer(
    Conf(config,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
  )

  def fireAndForget(record: ProducerRecord[String, String]) = producer.send(record)

  def asyncSend(record: ProducerRecord[String, String]) = {
    producer.sendWithCallback(record)(record => record match {
      case Success(data) => println(s"Offest: ${data.offset()}, Topic: ${data.topic()}, " +
        s"Partition: ${data.partition()}, Timestamp: ${data.timestamp()} ")
      case Failure(ex) => ex.printStackTrace()
    })
  }

  def close = producer.close()
}

object SimpleProducer {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.defaultApplication()
    val simpleProducer = new SimpleProducer(config)

    val topic = config.getString("topic")
    simpleProducer.fireAndForget(KafkaProducerRecord(topic, "Name", "Jimmys"))

    simpleProducer.asyncSend(KafkaProducerRecord(topic, "Age", "25"))

    Thread.sleep(1000)
  }
}