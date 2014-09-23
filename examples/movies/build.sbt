import AssemblyKeys._

assemblySettings

name := "movies-example"
      
version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.4"

jarName in assembly := "server.jar"

mainClass in assembly := Some ("movies.Main")

test in assembly := {}

libraryDependencies ++= Seq (
  "com.treode" %% "store" % "0.2.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "finatra" % "0.2.0-SNAPSHOT",
  "com.treode" %% "jackson" % "0.2.0-SNAPSHOT")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)
