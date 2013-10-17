import sbt._
import Keys._

import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.assemblySettings

object TreodeBuild extends Build {

  // Adds stub configuration.
  lazy val Stub = config ("stub") extend (Compile)
  lazy val CustomTest = config ("test") extend (Stub)

  lazy val commonSettings =
    inConfig (CustomTest) (Defaults.testSettings) ++
    Seq (

    organization := "com.treode",
    version := "0.1",
    scalaVersion := "2.10.3",

    ivyConfigurations := overrideConfigs (Compile, Stub, CustomTest) (ivyConfigurations.value),

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    unmanagedSourceDirectories in Stub <<=
      (baseDirectory ((base: File) => Seq (base / "stub"))),

    unmanagedSourceDirectories in CustomTest <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked"),

    testFrameworks += new TestFramework ("org.scalameter.ScalaMeterFramework"),

    libraryDependencies ++= Seq (
      "com.esotericsoftware.kryo" % "kryo" % "2.22",
      "com.google.code.findbugs" % "jsr305" % "1.3.9",
      "com.google.guava" % "guava" % "14.0.1",
      "com.yammer.metrics" % "metrics-core" % "3.0.0-BETA1",
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "org.slf4j" % "slf4j-simple" % "1.7.2"))

  lazy val standardSettings =
    commonSettings ++
    Seq (

    libraryDependencies ++= Seq (
      "com.github.axel22" %% "scalameter" % "0.3" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "test",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"))

  lazy val withStubSettings =
    inConfig (Stub) (Defaults.configSettings) ++
    commonSettings ++
    Seq (

    EclipseKeys.configurations := Set (Compile, Stub, CustomTest),

    libraryDependencies ++= Seq (
      "com.github.axel22" %% "scalameter" % "0.3" % "stub->default",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "stub->default",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "stub->default",
      "org.scalacheck" %% "scalacheck" % "1.10.1" % "stub->default"))

  /*
   * Separated to allow focused development.
   */

  lazy val pickle = Project ("pickle", file ("pickle"))
    .settings (standardSettings: _*)

  lazy val cluster = Project ("cluster", file ("cluster"))
    .dependsOn (pickle)
    .settings (withStubSettings: _*)

  lazy val store = Project ("store", file ("store"))
    .dependsOn (pickle, cluster % "compile;stub->stub")
    .settings (withStubSettings: _*)

  /*
   * Production Server
   */

  lazy val treodeSettings =
    standardSettings ++
    assemblySettings ++
  Seq (
    name := "server",
    test in assembly := {})

  lazy val server = Project ("server", file ("server"))
    .dependsOn (store)
    .settings (treodeSettings: _*)

  /*
   * System Test, separated to keep test code out of production.
   */

  lazy val systestSettings =
    standardSettings ++
    assemblySettings ++
  Seq (
    name := "systest",
    test in assembly := {})

  lazy val systest = Project ("systest", file ("systest"))
    .dependsOn (server, store)
    .settings (systestSettings: _*)

}
