package samples.scaladsl

import akka.Done
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import io.opentracing.Tracer
import org.apache.kafka.common.header.Headers
import io.opentracing.SpanContext
import io.opentracing.contrib.kafka.{HeadersMapExtractAdapter, SpanDecorator, TracingKafkaUtils}
import io.opentracing.propagation.Format
import io.opentracing.References
import io.opentracing.SpanContext
import io.opentracing.Tracer
import io.opentracing.tag.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.function.BiFunction

import io.opentracing.SpanContext
import io.opentracing.contrib.kafka.HeadersMapInjectAdapter
import io.opentracing.propagation.Format
import io.opentracing.util.GlobalTracer

import scala.concurrent.Future
import scala.util.{Failure, Success}


class FetchSpanStage[T, K, V](extract: T => ConsumerRecord[K, V]) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("in")
  val out: Outlet[T] = Outlet[T]("out")
  val shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {

    private val tracer: Tracer = GlobalTracer.get()

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          val record: ConsumerRecord[K, V] = extract(element)
          val operationName = "fetchSpan" //consumerSpanNameProvider.apply(consumerOper, record)
          val spanBuilder = tracer.buildSpan(operationName).withTag(Tags.SPAN_KIND.getKey, Tags.SPAN_KIND_CONSUMER)
          val parentContext = tracer.extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(record.headers, false))
          if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext)
          }
          val span = spanBuilder.start
          val scope = tracer.scopeManager().activate(span, true)
          push(out, element)
          scope.close()
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          failStage(ex)
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = pull(in)
      }
    )
  }
}

import io.opentracing.Span
import io.opentracing.tag.Tags
import java.io.PrintWriter
import java.io.StringWriter
import java.util
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord


object SpanDecorator {
  val COMPONENT_NAME = "java-kafka"
  val KAFKA_SERVICE = "kafka"

  /**
   * Called before record is sent by producer
   */
  def onSend[K, V](record: ProducerRecord[K, V], span: Span): Unit = {
    setCommonTags(span)
    Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic)
    if (record.partition != null) span.setTag("partition", record.partition)
  }

  /**
   * Called when record is received in consumer
   */
  def onResponse[K, V](record: ConsumerRecord[K, V], span: Span): Unit = {
    setCommonTags(span)
    span.setTag("partition", record.partition)
    span.setTag("topic", record.topic)
    span.setTag("offset", record.offset)
  }

  def onError(exception: Exception, span: Span): Unit = {
    Tags.ERROR.set(span, true)
    span.log(errorLogs(exception))
  }

  private def errorLogs(throwable: Throwable) = {
    val errorLogs = new util.HashMap[String, AnyRef](4)
    errorLogs.put("event", Tags.ERROR.getKey)
    errorLogs.put("error.kind", throwable.getClass.getName)
    errorLogs.put("error.object", throwable)
    errorLogs.put("message", throwable.getMessage)
    val sw = new StringWriter
    throwable.printStackTrace(new PrintWriter(sw))
    errorLogs.put("stack", sw.toString)
    errorLogs
  }

  private def setCommonTags(span: Span): Unit = {
    Tags.COMPONENT.set(span, COMPONENT_NAME)
    Tags.PEER_SERVICE.set(span, KAFKA_SERVICE)
  }
}
