import AssemblyKeys._

assemblySettings

name := "finatra-example"
      
version := "0.1.0"

scalaVersion := "2.10.4"

jarName in assembly := "server.jar"

mainClass in assembly := Some ("example1.Main")

test in assembly := {}

libraryDependencies ++= Seq (
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.3",
  "com.treode" %% "store" % "0.1.0" % "compile;test->stub",
  "com.twitter" %% "finatra" % "1.5.3")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)
