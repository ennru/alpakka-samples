/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package samples.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import samples.scaladsl.JsonFormats._
import spray.json._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

trait Helper {

  final val log = LoggerFactory.getLogger(getClass)

  // Testcontainers: start Kafka in Docker
  // [[https://hub.docker.com/r/confluentinc/cp-kafka/tags Available Docker images]]
  // [[https://docs.confluent.io/current/installation/versions-interoperability.html Kafka versions in Confluent Platform]]
  val kafka = new KafkaContainer("5.1.2") // contains Kafka 2.1.x
  kafka.start()
  val kafkaBootstrapServers = kafka.getBootstrapServers

//  def writeToKafka(topic: String, movies: immutable.Iterable[Movie])(implicit actorSystem: ActorSystem) = {
//    val kafkaProducerSettings = ProducerSettings(actorSystem, new IntegerSerializer, new StringSerializer)
//      .withBootstrapServers(kafkaBootstrapServers)
//
//    val producing: Future[Done] = Source(movies)
//      .throttle(1, 1.second)
//      .map { movie =>
//        log.debug("producing {}", movie)
//        new ProducerRecord(topic, Int.box(movie.id), movie.toJson.compactPrint)
//      }
//      .runWith(Producer.plainSink(kafkaProducerSettings))
//    producing.foreach(_ => log.info("Producing finished"))(actorSystem.dispatcher)
//    producing
//  }
//
//  def readFormKafka(topic: String)(implicit actorSystem: ActorSystem): Control = {
//    val settings = ConsumerSettings(actorSystem, new IntegerDeserializer, new StringDeserializer)
//      .withBootstrapServers(kafkaBootstrapServers)
//      .withGroupId("reader")
//      .withStopTimeout(0.seconds)
//    Consumer.plainSource(settings, Subscriptions.topics(topic))
//      .toMat(Sink.foreach(record => println("received " + record.value())))(Keep.left)
//      .run()
//  }

  def stopContainers() = {
    kafka.stop()
  }
}
