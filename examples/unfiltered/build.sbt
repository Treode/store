import AssemblyKeys._

assemblySettings

organization := "com.treode"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.11.2"

scalacOptions ++=
  Seq ("-deprecation", "-feature", "-optimize", "-unchecked", "-Ywarn-unused-import")

libraryDependencies ++= Seq (
  "com.jayway.restassured" % "rest-assured" % "2.3.4" % "test",
  "com.treode" %% "jackson" % "0.2.0-SNAPSHOT",
  "com.treode" %% "store" % "0.2.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "twitter-app" % "0.2.0-SNAPSHOT",
  "com.twitter" %% "util-app" % "6.22.1",
  "com.twitter" %% "util-logging" % "6.22.1",
  "net.databinder" %% "unfiltered-netty-server" % "0.8.2",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test")

jarName in assembly := "unfiltered-server.jar"

mainClass in assembly := Some ("example.Main")

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList ("META-INF", "io.netty.versions.properties") => MergeStrategy.last
    case x => old (x)
  }
}