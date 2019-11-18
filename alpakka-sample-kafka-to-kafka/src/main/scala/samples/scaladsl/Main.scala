/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.sample

// #imports

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka._
import akka.kafka.internal.{DefaultProducerStage, SourceWithOffsetContext}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.Producer.{committableSink, flexiFlow}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.lightbend.cinnamon.akka.stream.CinnamonAttributes
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.{SamplerConfiguration, SenderConfiguration}
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.Tracer
import io.opentracing.contrib.kafka.{TracingKafkaConsumer, TracingKafkaProducer}
import io.opentracing.util.GlobalTracer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import samples.scaladsl.{FetchSpanStage, Helper, Movie}
import spray.json._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
// #imports

object Main extends App with Helper {

  import samples.scaladsl.JsonFormats._

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val topic = "movies"
  val targetTopic = "movie-target"
  private val groupId = "docs-group"

  //
  //  val samplerConfig = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1)
  //  val senderConfig = SenderConfiguration.fromEnv()//.withAgentHost("localhost").withAgentPort(5775)
  //  val reporterConfig = new Configuration.ReporterConfiguration().withLogSpans(true).withFlushInterval(1000).withMaxQueueSize(10000).withSender(senderConfig)
  //
  ////  val samplerConfig = SamplerConfiguration.fromEnv.withType(ConstSampler.TYPE).withParam(1)
  ////  val reporterConfig = ReporterConfiguration.fromEnv.withLogSpans(true)
  //  val jaegerConfig = new Configuration("helloWorld").withSampler(samplerConfig).withReporter(reporterConfig)

  val tracer: Tracer = GlobalTracer.get()
  //  GlobalTracer.register(tracer)

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
//      .throttle(1, 1.second)
      .map { movie =>
        log.debug("producing {}", movie)
        new ProducerRecord(topic, Int.box(movie.id), movie.toJson.compactPrint)
      }
      .named("prod")
      .addAttributes(CinnamonAttributes.instrumented(reportByName = true, traceable = true))
      .runWith(Producer.plainSink(kafkaProducerSettings))
    producing.foreach(_ => log.info("Producing finished"))(actorSystem.dispatcher)
    producing
  }

  private def kafkaToKafka() = {
    // #flow
    val control: Consumer.DrainingControl[Done] =
      Consumer
        .committableSource(kafkaConsumerSettings, Subscriptions.topics(topic))
        .via(new FetchSpanStage[ConsumerMessage.CommittableMessage[Integer, String], Integer, String](m => m.record))
        //      Consumer
        //      .sourceWithOffsetContext(kafkaConsumerSettings, Subscriptions.topics(topic))
        .map(t => (t.record.value().parseJson.convertTo[Movie], t.committableOffset))
        .map { case (movie, offset) =>
          // do something interesting here
          movie.copy(title = movie.title + System.currentTimeMillis()) -> offset
        }
        .map[Envelope[Integer, String, ConsumerMessage.Committable]] { case (movie, offset) =>
          val movieJson = movie.toJson.compactPrint
          ProducerMessage.single(new ProducerRecord(targetTopic, Int.box(movie.id), movieJson), offset)
        }
        //        .map {
        //          case (env, offset) =>
        //            env.withPassThrough(offset)
        //        }
        .via(Flow.fromGraph(
          new DefaultProducerStage[Integer, String, Committable, Envelope[Integer, String, Committable], Results[Integer, String, Committable]](
            kafkaProducerSettings
          )
        ).named("producer"))
        .mapAsync[Results[Integer, String, Committable]](kafkaProducerSettings.parallelism)(identity)
        .map(_.passThrough)
        .groupedWeightedWithin(kafkaCommitterSettings.maxBatch, kafkaCommitterSettings.maxInterval)(_.batchSize)
        .via(Flow[immutable.Seq[Committable]]
          .map(CommittableOffsetBatch.apply)
          .named("batch offsets")
        )
        .mapAsyncUnordered(kafkaCommitterSettings.parallelism) { b =>
          b.commitInternal().map(_ => b)(ExecutionContexts.sameThreadExecutionContext)
        }
        //    .via(Committer.flow(kafkaCommitterSettings))
        .toMat(Sink.ignore)(Keep.both)
        //        .toMat(Committer.sink(kafkaCommitterSettings))(Keep.both)
        //        .toMat(Producer.committableSink(kafkaProducerSettings, kafkaCommitterSettings))(Keep.both)
//                .toMat(Producer.committableSinkWithOffsetContext(kafkaProducerSettings, kafkaCommitterSettings))(Keep.both)
        .mapMaterializedValue(Consumer.DrainingControl.apply)
        .named("copy")
        .addAttributes(CinnamonAttributes.instrumented(reportByName = true, traceable = true))
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
      .via(new FetchSpanStage[ConsumerRecord[Integer, String], Integer, String](identity))
      .toMat(Sink.foreach(record => println("received " + record.value())))(Keep.left)
      .named("check")
      .addAttributes(CinnamonAttributes.instrumented(reportByName = true, traceable = true))
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
