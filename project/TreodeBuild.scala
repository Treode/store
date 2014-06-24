/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys
import sbtassembly.Plugin.AssemblyKeys
import sbtassembly.Plugin.assemblySettings
import sbtunidoc.Plugin.{ScalaUnidoc, UnidocKeys, unidocSettings}

import sbt._
import AssemblyKeys._
import Keys._
import UnidocKeys._

object TreodeBuild extends Build {

  lazy val IntensiveTest = config ("intensive") extend (Test)
  lazy val PeriodicTest = config ("periodic") extend (Test)

  // Settings common to both projects with stubs and without stubs.
  // Adds production libraries to the "main" configuration.  Squashes
  // the source directory structure.
  lazy val commonPortion = Seq (

    organization := "com.treode",
    version := "0.1.0",
    scalaVersion := "2.10.4",

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked", 
      "-Yinline-warnings"),

    libraryDependencies <+= (scalaVersion) ("org.scala-lang" % "scala-reflect" % _),

    libraryDependencies ++= Seq (
      "com.codahale.metrics" % "metrics-core" % "3.0.2",
      "com.google.code.findbugs" % "jsr305" % "2.0.3",
      "com.google.guava" % "guava" % "17.0",
      "com.googlecode.javaewah" % "JavaEWAH" % "0.8.6",
      "com.nothome" % "javaxdelta" % "2.0.1",
      "joda-time" % "joda-time" % "2.3",
      "org.joda" % "joda-convert" % "1.2",
      "org.slf4j" % "slf4j-api" % "1.7.7",
      "org.slf4j" % "slf4j-simple" % "1.7.7"))

  // A portion of the settings for projects without stubs.  Adds
  // testing libraries to SBT's "test" configuration.
  lazy val standardPortion = Seq (

    ivyConfigurations := overrideConfigs (Compile, Test) (ivyConfigurations.value),

    EclipseKeys.configurations := Set (Compile, Test),

    testOptions in Test := Seq (
      Tests.Argument ("-l", "com.treode.tags.Intensive", "-oDF")),

    testOptions in IntensiveTest := Seq (
      Tests.Argument ("-n", "com.treode.tags.Intensive", "-oDF")),

    testOptions in PeriodicTest := Seq (
      Tests.Argument ("-n", "com.treode.tags.Periodic", "-oDF")),

    unmanagedSourceDirectories in Test <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    libraryDependencies ++= Seq (
      "org.scalamock" %% "scalamock-scalatest-support" % "3.1.RC1" % "test",
      "org.scalatest" %% "scalatest" % "2.2.0" % "test",
      "org.scalacheck" %% "scalacheck" % "1.11.4" % "test"))

  // Settings for projects without stubs.
  lazy val standardSettings =
    inConfig (IntensiveTest) (Defaults.testTasks) ++ 
    inConfig (PeriodicTest) (Defaults.testTasks) ++ 
    commonPortion ++
    standardPortion

  lazy val Stub = config ("stub") extend (Compile)
  lazy val TestWithStub = config ("test") extend (Stub)
  lazy val IntensiveTestWithStub = config ("intensive") extend (TestWithStub)
  lazy val PeriodicTestWithStub = config ("periodic") extend (TestWithStub)

  // A portion of the settings for projects with stubs.  Adds the
  // "stub" configuration and creates a replaces the SBT "test"
  // configuration with a new one that depends on the stubs.  Then
  // adds the testing libraries to the new "test" configuration.
  lazy val stubPortion = Seq (

    ivyConfigurations := overrideConfigs (Compile, Stub, TestWithStub) (ivyConfigurations.value),

    EclipseKeys.configurations := Set (Compile, Stub, TestWithStub),

    testOptions in TestWithStub := Seq (
      Tests.Argument ("-l", "com.treode.tags.Intensive", "-oDF")),

    testOptions in IntensiveTestWithStub := Seq (
      Tests.Argument ("-n", "com.treode.tags.Intensive", "-oDF")),

    testOptions in PeriodicTestWithStub := Seq (
      Tests.Argument ("-n", "com.treode.tags.Periodic", "-oDF")),

    publishArtifact in Stub := true,

    unmanagedSourceDirectories in Stub <<=
      (baseDirectory ((base: File) => Seq (base / "stub"))),

    unmanagedSourceDirectories in TestWithStub <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    libraryDependencies ++= Seq (
      "org.scalamock" %% "scalamock-scalatest-support" % "3.1.RC1" % "stub->default",
      "org.scalatest" %% "scalatest" % "2.2.0" % "stub->default",
      "org.scalacheck" %% "scalacheck" % "1.11.4" % "stub->default"))

  // Settings for projects with stubs.
  lazy val stubSettings =
    inConfig (Stub) (Defaults.configSettings) ++
    addArtifact (artifact in (Stub, packageBin), packageBin in Stub) ++
    inConfig (TestWithStub) (Defaults.testTasks) ++
    inConfig (IntensiveTestWithStub) (Defaults.testTasks) ++
    inConfig (PeriodicTestWithStub) (Defaults.testTasks) ++
    commonPortion ++
    stubPortion

  // Both the async and pickle projects depend on buffers, but someday
  // someone may want the async package without the picklers or vice
  // versa, so we have isolated the buffers into their own project.
  lazy val buffer = Project ("buffer", file ("buffer"))
    .configs (IntensiveTest, PeriodicTest)
    .settings (standardSettings: _*)

  // Separated because this may be useful on its own.
  lazy val pickle = Project ("pickle", file ("pickle"))
    .configs (IntensiveTest, PeriodicTest)
    .dependsOn (buffer)
    .settings (standardSettings: _*)

  // Separated because this may be useful on its own.
  lazy val async = Project ("async", file ("async"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub)
    .dependsOn (buffer, pickle % "test")
    .settings (stubSettings: _*)

  // Separated because it helped development.
  lazy val cluster = Project ("cluster", file ("cluster"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub)
    .dependsOn (async % "compile;stub->stub", pickle)
    .settings (stubSettings: _*)

  // Separated because it helped development.
  lazy val disk = Project ("disk", file ("disk"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub)
    .dependsOn (async % "compile;stub->stub", pickle)
    .settings (stubSettings: _*)

  // The main component that this repository and build provides.
  lazy val store = Project ("store", file ("store"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub)
    .dependsOn (cluster % "compile;stub->stub", disk % "compile;stub->stub")
    .settings (stubSettings: _*)

  lazy val jackson = Project ("jackson", file ("jackson"))
    .configs (IntensiveTest, PeriodicTest)
    .dependsOn (store)
    .settings (standardSettings: _*)
    .settings (

        libraryDependencies ++= Seq (
          "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.3",
          "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.3"))

  // A standalone server for system tests.  Separated to keep system
  // testing components out of production code (these components are
  // in the "default" Ivy configuration in this project).
  lazy val systest = Project ("systest", file ("systest"))
    .configs (IntensiveTest, PeriodicTest)
    .dependsOn (store)
    .settings (standardSettings: _*)
    .settings (assemblySettings: _*)
    .settings (
      name := "systest",
      test in assembly := {},
      publishArtifact := false)

  lazy val copyDocAssetsTask = taskKey [Unit] ("Copy doc assets")

  lazy val root = Project ("root", file ("."))
    .aggregate (buffer, pickle, async, cluster, disk, store, jackson, systest)
    .settings (unidocSettings: _*)
    .settings (

      name := "root",

      scalacOptions in (ScalaUnidoc, unidoc) ++= Seq (
        "-diagrams",
        "-doc-title", "TreodeDB 0.1.0", 
        "-doc-root-content", baseDirectory.value + "/doc/rootdoc.html"),

      unidocConfigurationFilter in (ScalaUnidoc, unidoc) := 
        inConfigurations (Compile, Stub),

      unidocProjectFilter in (ScalaUnidoc, unidoc) := 
        inAnyProject -- inProjects (systest),

      copyDocAssetsTask := {
        val sourceDir = file ("doc")
        val targetDir = (target in (ScalaUnidoc, unidoc)).value
        IO.copyDirectory (sourceDir, targetDir)
      },

      copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (unidoc in Compile))
}
