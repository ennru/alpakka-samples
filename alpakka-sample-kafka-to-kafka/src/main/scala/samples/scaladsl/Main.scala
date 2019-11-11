/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package samples.scaladsl

// #imports

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.lightbend.cinnamon.akka.stream.CinnamonAttributes
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.{SamplerConfiguration, SenderConfiguration}
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.Tracer
import io.opentracing.contrib.kafka.{TracingKafkaConsumer, TracingKafkaProducer}
import io.opentracing.util.GlobalTracer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import spray.json._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
// #imports

object Main extends App with Helper {

  import JsonFormats._

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val topic = "movies"
  val targetTopic = "movie-target"
  private val groupId = "docs-group"


  val samplerConfig = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1)
  val senderConfig = SenderConfiguration.fromEnv()//.withAgentHost("localhost").withAgentPort(5775)
  val reporterConfig = new Configuration.ReporterConfiguration().withLogSpans(true).withFlushInterval(1000).withMaxQueueSize(10000).withSender(senderConfig)

//  val samplerConfig = SamplerConfiguration.fromEnv.withType(ConstSampler.TYPE).withParam(1)
//  val reporterConfig = ReporterConfiguration.fromEnv.withLogSpans(true)
  val jaegerConfig = new Configuration("helloWorld").withSampler(samplerConfig).withReporter(reporterConfig)

  val tracer: Tracer = jaegerConfig.getTracer
  GlobalTracer.register(tracer)

  // #kafka-setup
  // configure Kafka consumer (1)
  val kafkaConsumerSettings = ConsumerSettings(actorSystem, new IntegerDeserializer, new StringDeserializer)
    .withConsumerFactory(settings => new TracingKafkaConsumer(ConsumerSettings.createKafkaConsumer(settings), tracer))
    .withBootstrapServers(kafkaBootstrapServers)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withStopTimeout(0.seconds)
  // #kafka-setup

  val kafkaProducerSettings = ProducerSettings(actorSystem, new IntegerSerializer, new StringSerializer)
    .withProducerFactory(settings => new TracingKafkaProducer(ProducerSettings.createKafkaProducer(settings), tracer))
    .withBootstrapServers(kafkaBootstrapServers)
  val kafkaCommitterSettings = CommitterSettings(actorSystem)

  def writeToKafka(topic: String, movies: immutable.Iterable[Movie])(implicit actorSystem: ActorSystem) = {
    val kafkaProducerSettings = ProducerSettings(actorSystem, new IntegerSerializer, new StringSerializer)
      .withProducerFactory(settings => new TracingKafkaProducer(ProducerSettings.createKafkaProducer(settings), tracer))
      .withBootstrapServers(kafkaBootstrapServers)

    val producing: Future[Done] = Source(movies)
      .throttle(1, 1.second)
      .map { movie =>
        log.debug("producing {}", movie)
        new ProducerRecord(topic, Int.box(movie.id), movie.toJson.compactPrint)
      }
      .runWith(Producer.plainSink(kafkaProducerSettings))
    producing.foreach(_ => log.info("Producing finished"))(actorSystem.dispatcher)
    producing
  }

  private def kafkaToKafka() = {
    // #flow
    val control: Consumer.DrainingControl[Done] = Consumer
      .sourceWithOffsetContext(kafkaConsumerSettings, Subscriptions.topics(topic))
      .map(_.value().parseJson.convertTo[Movie])
      .map { movie =>
        // do something interesting here
        movie.copy(title = movie.title + System.currentTimeMillis())
      }
      .map { movie =>
        val movieJson = movie.toJson.compactPrint
        ProducerMessage.single(new ProducerRecord(targetTopic, Int.box(movie.id), movieJson))
      }
      .toMat(Producer.committableSinkWithOffsetContext(kafkaProducerSettings, kafkaCommitterSettings))(Keep.both)
      .mapMaterializedValue(Consumer.DrainingControl.apply)
      .named("kafka-to-kafka")
      .addAttributes(CinnamonAttributes.instrumented(reportByName = true))
      .run()
    // #flow
    control
  }

  def readFormKafka(topic: String)(implicit actorSystem: ActorSystem): Control = {
    val settings = ConsumerSettings(actorSystem, new IntegerDeserializer, new StringDeserializer)
      .withConsumerFactory(settings => new TracingKafkaConsumer(ConsumerSettings.createKafkaConsumer(settings), tracer))
      .withBootstrapServers(kafkaBootstrapServers)
      .withGroupId("reader")
      .withStopTimeout(0.seconds)
    Consumer.plainSource(settings, Subscriptions.topics(topic))
      .toMat(Sink.foreach(record => println("received " + record.value())))(Keep.left)
      .run()
  }

  val movies = (0 to 10).map(id => Movie(id, "Psycho"))
  val writing: Future[Done] = writeToKafka(topic, movies)

  val control = kafkaToKafka()
  val received = readFormKafka(targetTopic)

  Await.result(writing, 10.seconds)
  // Let the read/write stream run a bit
  Thread.sleep(5.seconds.toMillis)
  val copyingFinished = control.drainAndShutdown()
  Await.result(copyingFinished, 10.seconds)

  Await.result(received.shutdown(), 2.seconds)

  stopContainers()
  actorSystem.terminate()
}
