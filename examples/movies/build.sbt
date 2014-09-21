import AssemblyKeys._

assemblySettings

name := "movies-example"
      
version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.4"

jarName in assembly := "server.jar"

mainClass in assembly := Some ("movies.Main")

test in assembly := {}

libraryDependencies ++= Seq (
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.4.0-rc2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.0-rc2",
  "com.treode" %% "store" % "0.2.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "jackson" % "0.2.0-SNAPSHOT",
  "com.twitter" %% "finatra" % "1.5.3")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)
