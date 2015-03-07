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

package com.treode.store.tier

import com.treode.disk.Position
import com.treode.store.{Residents, StoreConfig, StorePicklers, TableDigest, Window}

 private case class Tiers (tiers: Seq [Tier]) {

  def apply (i: Int): Tier =
    tiers (i)

  def size: Int =
    tiers.length

  def isEmpty: Boolean =
    tiers.isEmpty

  def overlaps (window: Window): Tiers =
    Tiers (tiers.filter (_.overlaps (window)))

  def maxGen: Long =
    if (tiers.isEmpty) 0 else tiers.head.gen

  def minGen: Long =
    if (tiers.isEmpty) Long.MaxValue else tiers.last.gen

  def gens: Seq [Long] =
    tiers map (_.gen)

  def estimate (other: Residents): Long =
    tiers .map (_.estimate (other)) .sum

  def active: Set [Long] =
    tiers .map (_.gen) .toSet

  def choose (gens: Set [Long], residents: Residents) (implicit config: StoreConfig): Tiers = {
    var selected = -1
    var bytes = 0L
    var i = 0
    while (i < tiers.length) {
      val tier = tiers (i)
      if (gens contains tier.gen)
        selected = i
      if (tier.residents.exodus (residents))
        selected = i
      if (tier.diskBytes < bytes)
        selected = i
      bytes += tier.diskBytes
      i += 1
    }
    new Tiers (tiers take (selected + 1))
  }

  def compact (keep: Long, tier: Tier): Tiers = {
    val bldr = Seq.newBuilder [Tier]
    bldr ++= tiers takeWhile (_.gen > tier.gen)
    if (tier.keys > 0)
      bldr += tier
    bldr ++= tiers dropWhile (_.gen >= keep)
    new Tiers (bldr.result)
  }

  def checkpoint (tier: Tier): Tiers = {
    assert (tier.gen > maxGen, s"$tier $this")
    new Tiers (tier +: tiers)
  }

  // TODO: Remove after release of 0.2.0.
  // This method remains to support replay of TierTable.Meta.
  def compare (that: Tiers): Int = {
    val _this = this.gens
    val _that = that.gens
    for ((i, j) <- _this zip _that; r = i - j)
      if (r < 0)
        return -1
      else if (r > 0)
        return 1
    if (_this.size < _that.size)
      -1
    else if (_this.size > _that.size)
      1
    else
      0
  }

  def digest: Seq [TableDigest.Tier] =
    tiers map (_.digest)

  override def toString: String =
    s"Tiers(\n   ${tiers mkString ",\n   "})"
}

private object Tiers extends Ordering [Tiers] {

  val empty: Tiers = new Tiers (Seq.empty)

  def apply (tier: Tier): Tiers =
    new Tiers (Array (tier))

  def compare (x: Tiers, y: Tiers): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (seq (Tier.pickler))
    .build (new Tiers (_))
    .inspect (_.tiers)
  }}
