import sbt._
import Keys._

import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.assemblySettings

object TreodeBuild extends Build {

  // Settings common to both projects with stubs and without stubs.
  // Adds production libraries to the "main" configuration.  Squashes
  // the source directory structure.
  lazy val commonPortion = Seq (

    organization := "com.treode",
    version := "0.1",
    scalaVersion := "2.10.3",

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    unmanagedSourceDirectories in CustomTest <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked", "-Yinline-warnings"),

    testFrameworks += new TestFramework ("org.scalameter.ScalaMeterFramework"),

    libraryDependencies <+= (scalaVersion) ("org.scala-lang" % "scala-reflect" % _),

    libraryDependencies ++= Seq (
      "com.codahale.metrics" % "metrics-core" % "3.0.1",
      "com.google.code.findbugs" % "jsr305" % "2.0.2",
      "com.google.guava" % "guava" % "15.0",
      "com.googlecode.javaewah" % "JavaEWAH" % "0.7.9",
      "com.nothome" % "javaxdelta" % "2.0.1",
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "org.slf4j" % "slf4j-simple" % "1.7.5"))

  // A portion of the settings for projects without stubs.  Adds
  // testing libraries to SBT's "test" configuration.
  lazy val standardPortion = Seq (

    libraryDependencies ++= Seq (
      "com.github.axel22" %% "scalameter" % "0.3" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "test",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"))

  // Settings for projects without stubs.
  lazy val standardSettings =
    commonPortion ++
    standardPortion

  lazy val Stub = config ("stub") extend (Compile)
  lazy val CustomTest = config ("test") extend (Stub)

  // A portion of the settings for projects with stubs.  Adds the
  // "stub" configuration and creates a replaces the SBT "test"
  // configuration with a new one that depends on the stubs.  Then
  // adds the testing libraries to the new "test" configuration.
  lazy val stubPortion = Seq (

    ivyConfigurations := overrideConfigs (Compile, Stub, CustomTest) (ivyConfigurations.value),

    unmanagedSourceDirectories in Stub <<=
      (baseDirectory ((base: File) => Seq (base / "stub"))),

    EclipseKeys.configurations := Set (Compile, Stub, CustomTest),

    libraryDependencies ++= Seq (
      "com.github.axel22" %% "scalameter" % "0.3" % "stub->default",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "stub->default",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "stub->default",
      "org.scalacheck" %% "scalacheck" % "1.10.1" % "stub->default"))

  // Settings for projects with stubs.
  lazy val stubSettings =
    inConfig (Stub) (Defaults.configSettings) ++
    inConfig (CustomTest) (Defaults.testSettings) ++
    commonPortion ++
    stubPortion

  // Both the async and pickle projects depend on buffers, but someday
  // someone may want the async package without the picklers or vice
  // versa, so we have isolated the buffers into their own project.
  lazy val buffer = Project ("buffer", file ("buffer"))
    .settings (standardSettings: _*)

  // Separated because this may be useful on its own.
  lazy val async = Project ("async", file ("async"))
    .dependsOn (buffer)
    .settings (stubSettings: _*)

  // Separated because this may be useful on its own.
  lazy val pickle = Project ("pickle", file ("pickle"))
    .dependsOn (buffer)
    .settings (standardSettings: _*)

  // Separated because it helped development.
  lazy val cluster = Project ("cluster", file ("cluster"))
    .dependsOn (async % "compile;stub->stub", pickle)
    .settings (stubSettings: _*)

  // Separated because it helped development.
  lazy val disk = Project ("disk", file ("disk"))
    .dependsOn (async % "compile;test->stub", pickle)
    .settings (standardSettings: _*)

  // The main component that this repository and build provides.
  lazy val store = Project ("store", file ("store"))
    .dependsOn (cluster % "compile;stub->stub", disk)
    .settings (stubSettings: _*)

  // A standalone server for system tests.  Separated to keep system
  // testing components out of production code (these components are
  // in the "default" Ivy configuration in this project).
  lazy val systestSettings =
    standardSettings ++
    assemblySettings ++
  Seq (
    name := "systest",
    test in assembly := {})

  lazy val systest = Project ("systest", file ("systest"))
    .dependsOn (store)
    .settings (systestSettings: _*)

  lazy val exampleSettings = Seq (

    organization := "com.treode",
    version := "0.1",
    scalaVersion := "2.10.3",

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    unmanagedSourceDirectories in CustomTest <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked", "-Yinline-warnings"),

    libraryDependencies ++= Seq (
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.1",
      "com.twitter" %% "finatra" % "1.5.2"),

    resolvers += "Twitter" at "http://maven.twttr.com")

  lazy val example1 = Project ("example1", file ("example1"))
    .dependsOn (store)
    .settings (exampleSettings: _*)
}
