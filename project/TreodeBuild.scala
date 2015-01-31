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

  // The intensive config runs only tests tagged intenstive.
  lazy val IntensiveTest = config ("intensive") extend (Test)

  // The periodic config runs only tests tagged periodic.
  lazy val PeriodicTest = config ("periodic") extend (Test)

  // The perf config runs only Scalameter tests.
  lazy val Perf = config ("perf") extend (Test)

  lazy val versionString = "0.3.0-SNAPSHOT"

  lazy val versionInfo = Seq (

    organization := "com.treode",
    version := versionString,
    scalaVersion := "2.11.4",
    crossScalaVersions := Seq ("2.10.4", "2.11.4"),

    // Use a local Scala installation if SCALA_HOME is set. Otherwise, download the Scala tools
    // per scalaVersion.
    scalaHome := Option (System.getenv ("SCALA_HOME")) map (file _))

  // Settings common to both projects with stubs and without stubs. Squashes the source directory
  // structure. Adds production libraries to the default config. Removes docs from Ivy artifacts in
  // favor of unidoc.
  lazy val commonPortion = Seq (

    unmanagedSourceDirectories in Compile <<=
      (baseDirectory ((base: File) => Seq (base / "src"))),

    scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked"),

    scalacOptions <++= scalaVersion map {
      case "2.10.4" => Seq.empty
      case "2.11.4" => Seq ("-Ywarn-unused-import")
    },

    libraryDependencies <+= scalaVersion ("org.scala-lang" % "scala-reflect" % _),

    libraryDependencies ++= Seq (
      "com.codahale.metrics" % "metrics-core" % "3.0.2",
      "com.google.code.findbugs" % "jsr305" % "3.0.0",
      "com.google.guava" % "guava" % "18.0",
      "com.googlecode.javaewah" % "JavaEWAH" % "0.9.0",
      "com.nothome" % "javaxdelta" % "2.0.1",
      "joda-time" % "joda-time" % "2.5",
      "org.joda" % "joda-convert" % "1.7",
      "org.slf4j" % "slf4j-api" % "1.7.7",
      "org.slf4j" % "slf4j-simple" % "1.7.7"),

    publishArtifact in (Compile, packageDoc) := false,

    publishTo :=
      Some (Resolver.file ("Staging Repo", file ("stage/ivy")) (Resolver.ivyStylePatterns)))

  // A portion of the settings for projects without stubs.  Adds testing libraries to SBT's
  // test config.
  lazy val standardPortion = Seq (

    ivyConfigurations :=
      overrideConfigs (Compile, Test, Perf) (ivyConfigurations.value),

    EclipseKeys.configurations :=
      Set (Compile, Test, Perf),

    testOptions in Test := Seq (
      Tests.Argument ("-l", "com.treode.tags.Intensive", "-oDF")),

    testOptions in IntensiveTest := Seq (
      Tests.Argument ("-n", "com.treode.tags.Intensive", "-oDF")),

    testOptions in PeriodicTest := Seq (
      Tests.Argument ("-n", "com.treode.tags.Periodic", "-oDF")),

    testFrameworks in Perf := Seq (
      new TestFramework ("org.scalameter.ScalaMeterFramework")),

    parallelExecution in Perf := false,

    testOptions in Perf := Seq.empty,

    unmanagedSourceDirectories in Test <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    libraryDependencies ++= Seq (
      "org.scalacheck" %% "scalacheck" % "1.12.0" % "test",
      "com.storm-enroute" %% "scalameter" % "0.6" % "perf",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test",
      "org.scalatest" %% "scalatest" % "2.2.2" % "test"))

  // Settings for projects without stubs.
  lazy val standardSettings =
    inConfig (IntensiveTest) (Defaults.testTasks) ++
    inConfig (PeriodicTest) (Defaults.testTasks) ++
    inConfig (Perf) (Defaults.testTasks) ++
    versionInfo ++
    commonPortion ++
    standardPortion

  // The stub config introduces classes for testing. These are not classes for testing within the
  // project, for those can reside in the test config. They are classes for dependents of the
  // project, but they do not belong in the production jar, otherwise they could reside in the
  // default config.
  lazy val Stub = config ("stub") extend (Compile)
  lazy val TestWithStub = config ("test") extend (Stub)
  lazy val IntensiveTestWithStub = config ("intensive") extend (TestWithStub)
  lazy val PeriodicTestWithStub = config ("periodic") extend (TestWithStub)
  lazy val PerfWithStub = config ("perf") extend (TestWithStub)

  // A portion of the settings for projects with stubs.  Adds the "stub" config and creates
  // a replaces the SBT "test" config with a new one that depends on the stubs.  Then
  // adds the testing libraries to the new "test" config.
  lazy val stubPortion = Seq (

    ivyConfigurations :=
      overrideConfigs (Compile, Stub, TestWithStub, PerfWithStub) (ivyConfigurations.value),

    EclipseKeys.configurations :=
      Set (Compile, Stub, TestWithStub, PerfWithStub),

    testOptions in TestWithStub := Seq (
      Tests.Argument ("-l", "com.treode.tags.Intensive", "-oDF")),

    testOptions in IntensiveTestWithStub := Seq (
      Tests.Argument ("-n", "com.treode.tags.Intensive", "-oDF")),

    testOptions in PeriodicTestWithStub := Seq (
      Tests.Argument ("-n", "com.treode.tags.Periodic", "-oDF")),

    testFrameworks in PerfWithStub := Seq (
      new TestFramework ("org.scalameter.ScalaMeterFramework")),

    parallelExecution in PerfWithStub := false,

    testOptions in PerfWithStub := Seq.empty,

    publishArtifact in Stub := true,

    unmanagedSourceDirectories in Stub <<=
      (baseDirectory ((base: File) => Seq (base / "stub"))),

    unmanagedSourceDirectories in TestWithStub <<=
      (baseDirectory ((base: File) => Seq (base / "test"))),

    libraryDependencies ++= Seq (
      "org.scalacheck" %% "scalacheck" % "1.12.0" % "stub->default",
      "com.storm-enroute" %% "scalameter" % "0.6" % "stub->default",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "stub->default",
      "org.scalatest" %% "scalatest" % "2.2.2" % "stub->default"))

  // Settings for projects with stubs.
  lazy val stubSettings =
    inConfig (Stub) (Defaults.configSettings) ++
    addArtifact (artifact in (Stub, packageBin), packageBin in Stub) ++
    inConfig (TestWithStub) (Defaults.testTasks) ++
    inConfig (IntensiveTestWithStub) (Defaults.testTasks) ++
    inConfig (PeriodicTestWithStub) (Defaults.testTasks) ++
    inConfig (PerfWithStub) (Defaults.testTasks) ++
    versionInfo ++
    commonPortion ++
    stubPortion

  // Both the async and pickle projects depend on buffers, but someday someone may want the async
  // package without the picklers or vice  versa, so we have isolated the buffers into their own
  // project.
  lazy val buffer = Project ("buffer", file ("buffer"))
    .configs (IntensiveTest, PeriodicTest, Perf)
    .settings (standardSettings: _*)

  // Separated because this may be useful on its own.
  lazy val pickle = Project ("pickle", file ("pickle"))
    .configs (IntensiveTest, PeriodicTest, Perf)
    .dependsOn (buffer)
    .settings (standardSettings: _*)

  // Separated because this may be useful on its own.
  lazy val async = Project ("async", file ("async"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub, PerfWithStub)
    .dependsOn (buffer, pickle % "test")
    .settings (stubSettings: _*)

  // Separated because it helped development.
  lazy val cluster = Project ("cluster", file ("cluster"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub, PerfWithStub)
    .dependsOn (async % "compile;stub->stub", pickle)
    .settings (stubSettings: _*)

  // Separated because it helped development.
  lazy val disk = Project ("disk", file ("disk"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub, PerfWithStub)
    .dependsOn (async % "compile;stub->stub", pickle)
    .settings (stubSettings: _*)

  // The main component that this build provides.
  lazy val store = Project ("store", file ("store"))
    .configs (IntensiveTestWithStub, PeriodicTestWithStub, PerfWithStub)
    .dependsOn (cluster % "compile;stub->stub", disk % "compile;stub->stub")
    .settings (stubSettings: _*)

  // Separated because not everyone wants it and its dependencies.
  lazy val jackson = Project ("jackson", file ("jackson"))
    .configs (IntensiveTest, PeriodicTest, Perf)
    .dependsOn (store)
    .settings (standardSettings: _*)
    .settings (

        libraryDependencies ++= Seq (
          "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.4.4",
          "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.4"))

  // Separated because not everyone wants it and its dependencies.
  lazy val twitterServer = Project ("twitter-server", file ("twitter-server"))
    .configs (IntensiveTest, PeriodicTest, Perf)
    .dependsOn (store, jackson)
    .settings (standardSettings: _*)
    .settings (

        resolvers += "Twitter" at "http://maven.twttr.com",

        libraryDependencies ++= Seq (
          "com.jayway.restassured" % "rest-assured" % "2.4.0" % "test",
          "com.twitter" %% "twitter-server" % "1.9.0"))

  // A standalone server for system tests.  Separated to keep system testing components out of
  // production code (these components are in the default config in this project).
  lazy val systest = Project ("systest", file ("systest"))
    .configs (IntensiveTest, PeriodicTest, Perf)
    .dependsOn (store)
    .settings (standardSettings: _*)
    .settings (assemblySettings: _*)
    .settings (

      name := "systest",

      test in assembly := {},

      publishLocal := {},
      publish := {})

  lazy val copyDocAssetsTask = taskKey [Unit] ("Copy doc assets")

  // The doc project includes everything.
  lazy val doc = Project ("doc", file ("doc"))
    .aggregate (buffer, pickle, async, cluster, disk, store, jackson, twitterServer)
    .settings (versionInfo: _*)
    .settings (unidocSettings: _*)
    .settings (

      name := "doc",

      scalacOptions in (ScalaUnidoc, unidoc) ++= Seq (
        "-diagrams",
        "-doc-title", "TreodeDB " + versionString,
        "-doc-root-content", "doc/rootdoc.html"),

      unidocConfigurationFilter in (ScalaUnidoc, unidoc) :=
        inConfigurations (Compile, Stub),

      unidocProjectFilter in (ScalaUnidoc, unidoc) :=
        inAnyProject -- inProjects (systest),

      copyDocAssetsTask := {
        val sourceDir = file ("doc/assets")
        val targetDir = (target in (ScalaUnidoc, unidoc)).value
        IO.copyDirectory (sourceDir, targetDir)
      },

      copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (unidoc in Compile),

      publishLocal := {},
      publish := {})

  // The root project includes everything.
  lazy val root = Project ("root", file ("."))
    .aggregate (buffer, pickle, async, cluster, disk, store, jackson, twitterServer)
    .settings (versionInfo: _*)
    .settings (

      name := "root",

      publishLocal := {},
      publish := {})
}
