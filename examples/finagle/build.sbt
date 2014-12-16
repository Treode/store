import AssemblyKeys._
import com.atlassian.labs.gitstamp.GitStampPlugin._

assemblySettings

gitStampSettings

name := "finagle-example"

version := "0.3.0-SNAPSHOT"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq (
  "com.jayway.restassured" % "rest-assured" % "2.4.0" % "test",
  "com.treode" %% "jackson" % "0.3.0-SNAPSHOT",
  "com.treode" %% "store" % "0.3.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "twitter-server" % "0.3.0-SNAPSHOT",
  "com.twitter" %% "finagle-http" % "6.24.0",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)

jarName in assembly := "finagle-server.jar"

mainClass in assembly := Some ("example.Main")

test in assembly := {}
