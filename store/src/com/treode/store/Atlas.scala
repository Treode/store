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

import java.util.{Arrays, Objects}
import com.treode.cluster.{HostId, RumorDescriptor}
import com.treode.pickle.Pickler

import Atlas.shrink
import Cohort.{Issuing, Moving, Settled}
import Integer.highestOneBit

class Atlas private (
    val cohorts: Array [Cohort],
    val version: Int
) {

  private val mask = cohorts.length - 1

  private def _arraysEqual (x: Array [Cohort], y: Array [Cohort]): Boolean =
      Arrays.equals (x.asInstanceOf [Array [Object]], y.asInstanceOf [Array [Object]])

  private def _arrayHash (x: Array [Cohort]): Int =
      Arrays.hashCode (x.asInstanceOf [Array [Object]])

  def place (id: Int): Int =
    id & mask

  def place [A] (p: Pickler [A], v: A): Int =
    place (p.murmur32 (v))

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def locate [A] (p: Pickler [A], v: A): Cohort =
    locate (p.murmur32 (v))

  def quorum (hosts: Set [HostId]): Boolean =
    cohorts forall (_.quorum (hosts))

  def awaiting (hosts: Set [HostId]): Set [HostId] =
    cohorts.map (_.hosts -- hosts) .fold (Set.empty) (_ ++ _)

  def settled: Boolean =
    cohorts forall (_.settled)

  def issuing: Boolean =
    cohorts exists (_.issuing)

  def moving: Boolean =
    cohorts exists (_.moving)

  /** Determine which cohorts reside on the given host. */
  private [store] def residents (host: HostId): Residents = {
    val nums = for ((c, i) <- cohorts.zipWithIndex; if c.hosts contains host) yield i
    new Residents (nums.toSet, cohorts.size - 1)
  }

  /** Given a slice, determine how many shards of the slice each peer hosts. */
  def hosts (slice: Slice): Seq [(HostId, Int)] = {
    Stream
        .iterate (slice.slice & mask) (_ + slice.nslices)
        .takeWhile (_ < cohorts.length)
        .map (cohorts (_) .hosts)
        .flatten
        .groupBy (x => x)
        .toSeq
        .map {case (host, count) => (host, count.length)}
  }

  def change (cohorts: Array [Cohort]): Option [Atlas] = {
    require (
        highestOneBit (cohorts.length) == cohorts.length,
        "Number of cohorts must be a power of two.")

    val mask = cohorts.length - 1
    val size = math.max (cohorts.length, this.cohorts.length)
    val next = Array.tabulate [Cohort] (size) { i =>
      val before = this.cohorts (i & this.mask)
      val after = cohorts (i & mask)
      if (before.target == after.target)
        if (before.settled)
          Settled (before.target)
        else
          before
      else if (before.origin == after.target)
        Settled (after.target)
      else
        Issuing (before.origin, after.target)
    }

    if (_arraysEqual (this.cohorts, next))
      return None

    Some (Atlas (shrink (next), version + 1))
  }

  private [store] def advance (receipts: Map [HostId, Int], moves: Map [HostId, Int]): Option [Atlas] = {

    val current = receipts.filter (_._2 == version) .keySet
    if (!quorum (current))
      return None

    val moved = moves.filter (_._2 == version) .keySet
    var changed = false
    val next =
      for (cohort <- cohorts) yield
        cohort match {
          case Issuing (origin, targets) =>
            changed = true
            Moving (origin, targets)
          case Moving (origin, targets) if cohort.quorum (moved) =>
            changed = true
            Settled (targets)
          case _ =>
            cohort
      }
    if (!changed)
      return None

    Some (Atlas (shrink (next), version + 1))
  }

  override def equals (other: Any): Boolean =
    other match {
      case that: Atlas =>
        _arraysEqual (cohorts, that.cohorts) && version == that.version
      case _ =>
        false
    }

  override def hashCode: Int =
    Objects.hashCode (_arrayHash (cohorts), version)

  override def toString: String =
    s"Atlas($version,\n   ${cohorts mkString "\n    "})"
}

object Atlas {

  private [store] def shrink (cohorts: Array [Cohort]): Array [Cohort] = {
    val groups = (0 until cohorts.length) groupBy (cohorts.apply (_))
    val sizes = groups.values.map (_.size)  .toSet
    if (sizes.size != 1 || sizes.head == 1)
      return cohorts
    val reduction = sizes.head
    if (reduction != highestOneBit (reduction))
      return cohorts
    val size = cohorts.length / reduction
    val mask = size - 1
    if (groups.map (_._2.map (_ & mask) .toSet.size) .toSet.size != 1)
      return cohorts
    Arrays.copyOf (cohorts, size)
  }

  def apply (cohorts: Array [Cohort], version: Int): Atlas = {

    require (
        highestOneBit (cohorts.length) == cohorts.length,
        "Number of cohorts must be a power of two.")
    require (
        version > 0,
        "Atlas version must be positive.")

    new Atlas (cohorts, version)
  }

  val empty = new Atlas (Array (Cohort.empty), 0)

  val pickler = {
    import StorePicklers._
    wrap (array (cohort), uint)
    .build (v => new Atlas (v._1, v._2))
    .inspect (v => (v.cohorts, v.version))
  }

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, atlas)
  }

  val received = {
    import StorePicklers._
    RumorDescriptor (0x6E73ED5CBD7E357CL, uint)
  }

  val moved = {
    import StorePicklers._
    RumorDescriptor (0x24111C0F37C3C0E1L, uint)
  }}
