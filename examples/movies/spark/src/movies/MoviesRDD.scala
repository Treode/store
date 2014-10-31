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

package movies

import java.net.{URI, URL}
import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.{Partition => SparkPartition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import movies.{AnalyticsModel => AM}

private class MoviesRDD [T: ClassTag] (
  sc: SparkContext,
  url: URL,
  nslices: Int
) extends RDD [T] (sc, Nil) {

  def compute (split: SparkPartition, context: TaskContext): Iterator [T] = {
    val clazz = implicitly [ClassTag [T]] .wrap.runtimeClass
    val array = textJson.readValue (url, clazz) .asInstanceOf [Array [T]]
    array.iterator
  }

  protected def getPartitions: Array [SparkPartition] =
    Array.tabulate (nslices) (MoviesPartition (_))
}

object MoviesRDD {

  def movies (sc: SparkContext, host: URI, nslices: Int): RDD [AM.Movie] =
    new MoviesRDD (sc, host.resolve ("rdd/movies").toURL, nslices)

  def movies (sc: SparkContext, host: String, nslices: Int): RDD [AM.Movie] =
    movies (sc, new URI (host), nslices)

  def actors (sc: SparkContext, host: URI, nslices: Int): RDD [AM.Actor] =
    new MoviesRDD (sc, host.resolve ("rdd/actors").toURL, nslices)

  def actors (sc: SparkContext, host: String, nslices: Int): RDD [AM.Actor] =
    actors (sc, new URI (host), nslices)

  def roles (sc: SparkContext, host: URI, nslices: Int): RDD [AM.Role] =
    new MoviesRDD (sc, host.resolve ("rdd/movies").toURL, nslices)

  def roles (sc: SparkContext, host: String, nslices: Int): RDD [AM.Role] =
    roles (sc, new URI (host), nslices)
}
