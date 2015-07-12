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

import java.net.{URI, URL}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{Partition => SparkPartition, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import movies.{AnalyticsModel => AM}

package object movies {

  private [movies] case class MoviesPartition (index: Int) extends SparkPartition

  private [movies] val textJson = new ObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (new JodaModule)

  private [movies] def urlOfPartition (uri: URI, slice: Int, nslices: Int): URL = {
    assert (0 <= slice && slice < nslices)
    val ident = new URI (
      uri.getScheme,
      uri.getUserInfo,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      s"slice=$slice&nslices=$nslices", // query
      uri.getFragment)
    ident.toURL
  }

  private [movies] def urlOfWindow (uri: URI, slice: Int, nslices: Int, since: Long, until: Long): URL = {
    assert (0 <= slice && slice < nslices)
    val ident = new URI (
      uri.getScheme,
      uri.getUserInfo,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      s"slice=$slice&nslices=$nslices&since=$since&until=$until", // query
      uri.getFragment)
    ident.toURL
  }

  private [movies] def resolve (base: URI, path: String): URI =
    if (base.getPath == "")
      base.resolve ("/") .resolve (path)
    else
      base.resolve (path)

  implicit class RichSparkContext (sc: SparkContext) {

    def movies (base: URI, nslices: Int): RDD [AM.Movie] =
      new MoviesRDD (sc, resolve (base, "rdd/movies"), nslices)

    def movies (base: String, nslices: Int): RDD [AM.Movie] =
      movies (new URI (base), nslices)

    def actors (base: URI, nslices: Int): RDD [AM.Actor] =
      new MoviesRDD (sc, resolve (base, "rdd/actors"), nslices)

    def actors (base: String, nslices: Int): RDD [AM.Actor] =
      actors (new URI (base), nslices)

    def roles (base: URI, nslices: Int): RDD [AM.Role] =
      new MoviesRDD (sc, resolve (base, "rdd/roles"), nslices)

    def roles (base: String, nslices: Int): RDD [AM.Role] =
      roles (new URI (base), nslices)
  }

  implicit class RichStreamingContext (ssc: StreamingContext) {

    def movies (base: URI, nslices: Int): DStream [AM.Movie] =
      new MoviesDStream (ssc, resolve (base, "rdd/movies"), nslices)

    def movies (base: String, nslices: Int): DStream [AM.Movie] =
      movies (new URI (base), nslices)

    def actors (base: URI, nslices: Int): DStream [AM.Actor] =
      new MoviesDStream (ssc, resolve (base, "rdd/actors"), nslices)

    def actors (base: String, nslices: Int): DStream [AM.Actor] =
      actors (new URI (base), nslices)

    def roles (base: URI, nslices: Int): DStream [AM.Role] =
      new MoviesDStream (ssc, resolve (base, "rdd/roles"), nslices)

    def roles (base: String, nslices: Int): DStream [AM.Role] =
      roles (new URI (base), nslices)
  }}
