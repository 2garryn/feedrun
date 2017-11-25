name := "feed-system"

version := "1.0"

scalaVersion := "2.12.1"

val phantomVersion = "2.15.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.6",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.6" % Test,
 // "com.typesafe.akka" %% "akka-http" % "10.0.10",
  // Only when running against Akka 2.5 explicitly depend on akka-streams in same version as akka-actor
  "com.typesafe.akka" %% "akka-stream" % "2.5.4", // or whatever the latest version is
  "com.typesafe.akka" %% "akka-actor"  % "2.5.4", // or whatever the latest version is
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.2",
  "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.3.2",
  "com.datastax.cassandra" % "cassandra-driver-extras" % "3.3.2",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0",
  "commons-codec" % "commons-codec" % "1.10"


)