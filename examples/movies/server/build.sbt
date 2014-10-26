import AssemblyKeys._

assemblySettings

name := "movies-example"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.4"

unmanagedSourceDirectories in Compile <<=
  (baseDirectory ((base: File) => Seq (base / "src")))


    unmanagedSourceDirectories in Test <<=
      (baseDirectory ((base: File) => Seq (base / "test")))

libraryDependencies ++= Seq (
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.4.2",
  "com.treode" %% "store" % "0.2.0-SNAPSHOT" % "compile;test->stub",
  "com.treode" %% "finatra" % "0.2.0-SNAPSHOT",
  "com.treode" %% "jackson" % "0.2.0-SNAPSHOT")

resolvers += "Twitter" at "http://maven.twttr.com"

resolvers += Resolver.url (
  "treode-oss",
  new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)

jarName in assembly := "server.jar"

mainClass in assembly := Some ("movies.Main")

test in assembly := {}
