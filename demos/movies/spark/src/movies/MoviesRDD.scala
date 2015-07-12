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

import java.lang.Integer.highestOneBit
import java.net.{URI, URL}
import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.{Partition => SparkPartition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

private class MoviesRDD [T: ClassTag] (
  sc: SparkContext,
  uri: URI,
  nslices: Int
) extends RDD [T] (sc, Nil) {

  require (nslices == highestOneBit (nslices), "Number of slices must be a power of two.")

  private val clazz = implicitly [ClassTag [T]] .wrap.runtimeClass

  def url (slice: Int): URL =
    urlOfPartition (uri, slice, nslices)

  def compute (split: SparkPartition, context: TaskContext): Iterator [T] = {
    val slice = split.asInstanceOf [MoviesPartition] .index
    val array = textJson.readValue (url (slice), clazz) .asInstanceOf [Array [T]]
    array.iterator
  }

  protected def getPartitions: Array [SparkPartition] =
    Array.tabulate (nslices) (MoviesPartition (_))

  // TODO: Use the /hosts URI to get the preferred locations.
  override protected def getPreferredLocations (split: SparkPartition): Seq [String] =
    super.getPreferredLocations (split)
}
