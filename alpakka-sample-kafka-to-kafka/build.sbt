
organization := "com.lightbend.akka"
version := "1.3.0"
scalaVersion := Dependencies.scalaVer
libraryDependencies ++= Dependencies.dependencies

resolvers += Resolver.bintrayRepo("akka", "snapshots")

enablePlugins (Cinnamon)
cinnamon in run := true

libraryDependencies ++= Seq(
  Cinnamon.library.cinnamonCHMetrics,
  Cinnamon.library.cinnamonAkka,
  Cinnamon.library.cinnamonAkkaStream
)
