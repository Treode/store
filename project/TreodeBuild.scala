import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.assemblySettings

object TreodeBuild extends Build {

  lazy val standardSettings = Seq (

    organization := "com.treode",
    version := "0.1",
    scalaVersion := "2.10.2",

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    unmanagedSourceDirectories in Test <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-unchecked"),

    testOptions in Test ++= Seq (Tests.Argument ("-oDF")),

    testOptions in Test ++= (
      if (System.getProperty ("large", "false") == "true")
        Seq (Tests.Argument ("-n"), Tests.Argument ("LargeTest"))
      else
        Seq (Tests.Argument ("-l"), Tests.Argument ("LargeTest"))),

    libraryDependencies ++= Seq (
      "com.google.code.findbugs" % "jsr305" % "1.3.9",
      "com.google.guava" % "guava" % "14.0.1",
      "com.yammer.metrics" % "metrics-core" % "3.0.0-BETA1",
      "org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "org.slf4j" % "slf4j-simple" % "1.7.2"))

  /*
   * Separated to allow focused development.
   */

  lazy val store = Project ("store", file ("store"))
    .settings (standardSettings: _*)

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
