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

import com.treode.cluster.HostId
import com.treode.pickle.Pickler

class Residents private [store] (
    private val nums: Set [Int],
    private val mask: Int
) {

  def contains (id: Int): Boolean =
    nums contains (id & mask)

  def contains [A] (p: Pickler [A], v: A): Boolean =
    contains (p.murmur32 (v) & mask)

  def stability (other: Residents): Double = {
    val adjust =
      if (mask == other.mask)
        nums
      else if  (mask < other.mask)
        (0 to other.mask) .filter (i => nums contains (i & mask)) .toSet
      else
        nums .map (_ & other.mask) .toSet
    if (adjust.size == 0)
      1.0D
    else
      (adjust intersect other.nums).size.toDouble / adjust.size.toDouble
  }

  def exodus (other: Residents) (implicit config: StoreConfig): Boolean =
    1 - stability (other) > config.exodusThreshold

  override def toString = s"Residents($nums, $mask)"
}

object Residents {

  val all = new Residents (Set (0), 0)

  val pickler = {
    import StorePicklers._
    wrap (set (uint), uint)
    .build (v => new Residents (v._1, v._2))
    .inspect (v => (v.nums, v.mask))
  }}
