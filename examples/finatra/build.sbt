import AssemblyKeys._

assemblySettings

name := "finatra-example"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq (
  "com.treode" %% "store" % "0.2.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "finatra" % "0.2.0-SNAPSHOT",
  "com.treode" %% "jackson" % "0.2.0-SNAPSHOT")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)

jarName in assembly := "finatra-server.jar"

mainClass in assembly := Some ("example.Main")

test in assembly := {}
