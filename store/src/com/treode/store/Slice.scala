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

package com.treode.store

import java.lang.Integer.highestOneBit

import com.treode.pickle.Pickler

/** A slice of a table.
  *
  * Slices partition the rows of the table, and they do so aware of the atlas.  Slices let scans
  * run in parallel on different sets of hosts.  To determine which hosts serve some of the
  * replicas in a slice, use the `hosts` method in [[Store]].
  *
  * <img src="../../../img/slices.png"></img>
  *
  * @constructor Create a slice.
  * @param slice This slice; must be between 0 inclusive and nslices exclusive.
  * @param nslices The total number of slices; must be a power of two.
  */
case class Slice (slice: Int, nslices: Int) {
  require (
      1 <= nslices && highestOneBit (nslices) == nslices,
      "Number of slices must be a power of two and at least one.")
  require (
      0 <= slice && slice < nslices,
      "The slice must be between 0 (inclusive) and the number of slices (exclusive)")

  def contains (n: Int): Boolean =
    (n & (nslices-1)) == slice

  def contains [A] (p: Pickler [A], v: A): Boolean =
    contains (p.murmur32 (v))
}

object Slice {

  val all = Slice (0, 1)

  val pickler = {
    import StorePicklers._
    wrap (tuple (uint, uint))
    .build (v => Slice (v._1, v._2))
    .inspect (v => (v.slice, v.nslices))
  }}
