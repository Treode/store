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
import java.net.URI
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream

private class MoviesDStream [T: ClassTag] (
  _ssc: StreamingContext,
  uri: URI,
  nslices: Int
) extends InputDStream [T] (_ssc) {

  require (nslices == highestOneBit (nslices), "Number of slices must be a power of two.")

  @volatile private var lastTime: Time = new Time (0)

  def start(): Unit = ()

  def stop(): Unit = ()

  def compute (time: Time): Option [RDD [T]] = {
    val since = lastTime.milliseconds * 1000
    val until = time.milliseconds * 1000
    val batch = new MoviesDBatch [T] (_ssc.sparkContext, uri, nslices, since, until)
    lastTime = time
    Some (batch)
  }}
