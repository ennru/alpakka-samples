import sbt._

object Dependencies {
  val scalaVer = "2.13.1"
  // #deps
  val AkkaVersion = "2.6.0"
  val AlpakkaVersion = "1.1.2"
  val AlpakkaKafkaVersion = "1.1.0+19-2793361d"

  // #deps

  val dependencies = List(
  // #deps
    "com.typesafe.akka" %% "akka-stream-kafka" % AlpakkaKafkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    // Tracing
    "org.apache.kafka" % "kafka-clients" % "2.3.1",
    //"io.opentracing" % "opentracing-api" % "0.33.0",
    "io.jaegertracing" % "jaeger-client" % "0.32.0",
    "io.opentracing.contrib" % "opentracing-kafka-client" % "0.0.20",
    // for JSON in Scala
    "io.spray" %% "spray-json" % "1.3.5",
    // for JSON in Java
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.10.0",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.10.0",
    // Logging
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
  // #deps
    "org.testcontainers" % "kafka" % "1.12.3"
  )
}
