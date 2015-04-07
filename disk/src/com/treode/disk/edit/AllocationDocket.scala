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

package com.treode.disk.edit

import com.treode.disk.{GenerationDocket, ObjectId, TypeId}

import AllocationDocket.DocketId

/** A convenient facade for a `Map [(TypeId, ObjectId, Long, Int, Int), Long]`, that is a
  * `Map [(TypeId, ObjectId, Generation, DiskNumber, SegmentNumber), Long]`. It tracks the bytes
  * consumed in each disk segment by an object's generation.
  *
  * The `AllocationDocket` behaves as a single map, but it's implemented as nested maps. This
  * allows the pickler to write a TypeId once, and then an ObjectId once per TypeId, and then
  * the allocations. If `AllocationDocket` was just one map, and the pickler serialized that
  * directly, then it would write the TypeId once per object per generation per disk per segment,
  * and it would write the ObjectId once per generation per disk per segment. That adds up; the
  * nested design avoids the repitition.
  */
private class AllocationDocket private (
  private var pages: Map [Long, Map [Long, Map [Long, Map [Int, Map [Int, Long]]]]]
) extends Iterable [(DocketId, Long)] with Traversable [(DocketId, Long)] {

  def this() = this (Map.empty)

  /** Get the bytes allocated for the object's generation on the disk's segment. */
  def apply (typ: TypeId, obj: ObjectId, gen: Long, disk: Int, seg: Int): Long =
    (for {
      objs <- pages.get (typ.id)
      gens <- objs.get (obj.id)
      disks <- gens.get (gen)
      segs <- disks.get (disk)
      seg <- segs.get (seg)
    } yield seg) .getOrElse (0L)

  /** Add to the allocation for the object's generation on the disk's segment. */
  def alloc (typ: TypeId, obj: ObjectId, gen: Long, disk: Int, seg: Int, b1: Long) {
    if (b1 == 0) return
    val objs = pages getOrElse (typ.id, Map.empty)
    val gens = objs getOrElse (obj.id, Map.empty)
    val disks = gens getOrElse (gen, Map.empty)
    val segs = disks getOrElse (disk, Map.empty)
    val b0 = segs getOrElse (seg, 0L)
    pages =
      pages + (typ.id -> (
        objs + (obj.id -> (
          gens + (gen -> (
            disks + (disk -> (
              segs + (seg -> (b0 + b1))))))))))
  }

  /** Add to the allocations for the disk's segment'. */
  def alloc (disk: Int, seg: Int, tally: PageTally): Unit =
    for ((page, bytes) <- tally)
      alloc (page.typ, page.obj, page.gen, disk, seg, bytes)

  /** Add to the allocations. */
  def alloc (docket: AllocationDocket): Unit =
    for ((page, bytes) <- docket)
      alloc (page.typ, page.obj, page.gen, page.disk, page.seg, bytes)

  private def freed (gens: Map [Long, Map [Int, Map [Int, Long]]]): SegmentDocket = {
    var cs = new SegmentDocket
    for {
      (gen, disks) <- gens.toIterable
      (disk, segs) <- disks
    } cs.add (disk, segs.keys)
    for {
      (typ, objs) <- pages
      (obj, gens) <- objs
      (gen, disks) <- gens
      (disk, used0) <- disks
    } cs.remove (disk, used0.keys)
    cs
  }

  /** Subtract the allocations for the object's generations. */
  def free (typ: TypeId, obj: ObjectId, gens: Set [Long]): SegmentDocket = {
    (for {
      objs0 <- pages.get (typ.id)
      gens0 <- objs0.get (obj.id)
    } yield {
      val gens1 = gens0 -- gens
      val objs1 = if (gens1.isEmpty) objs0 - obj.id else objs0 + (obj.id -> gens1)
      pages = if (objs1.isEmpty) pages - typ.id else pages + (typ.id -> objs1)
      freed (gens0 filter (gens contains _._1))
    }) .getOrElse (SegmentDocket.empty)
  }

  /** Remove unclaimed generations for the object.
    *
    * Finds allocations for the object, and removes those generations that are not claimed. This
    * changes the map only for the identified object, not for any other objects.
    */
  def claim (typ: TypeId, obj: ObjectId, gens: Set [Long]): Unit =
    for {
      objs0 <- pages.get (typ.id)
      gens0 <- objs0.get (obj.id)
    } yield {
      val gens1 = gens0 filter (gens contains _._1)
      val objs1 = if (gens1.isEmpty) objs0 - obj.id else objs0 + (obj.id -> gens1)
      pages = if (objs1.isEmpty) pages - typ.id else pages + (typ.id -> objs1)
    }

  /** Remove unclaimed generations.
    *
    * Removes unclaimed generations of claimed objects, and removes all generations of unclaimed
    * objects.
    *
    * For objects that are listed in `claims`, removes those generations of those objects that are
    * not claimed. For objects that are not listed in `claims`, removes all generations of those
    * objects.
    */
  def claim (claims: GenerationDocket): Unit = {
    for ((id, grps) <- claims)
      claim (id.typ.id, id.obj.id, grps)
    for {
      (typ, objs0) <- pages
      (obj, gens) <- objs0
      if !(claims contains (typ, obj))
    } {
      val objs1 = objs0 - obj
      pages = if (objs1.isEmpty) pages - typ else pages + (typ -> objs1)
    }}

  /** Determine which disk segments hold allocations. */
  def claimed: SegmentDocket = {
    var claimed = new SegmentDocket
    for {
      (typ, objs) <- pages.iterator
      (obj, gens) <- objs.iterator
      (gen, disks) <- gens.iterator
      (disk, segs) <- disks.iterator
    } claimed.add (disk, segs.keys)
    claimed
  }

  /** Determine which generations of which objects must be compacted to drain the disks. */
  def drain (dno: Set [Int]): GenerationDocket = {
    val drains = new GenerationDocket
    for {
      (typ, objs) <- pages
      (obj, grps) <- objs
      (grp, disks) <- grps
      if disks.keys exists (dno contains _)
    } drains.add (typ, obj, grp)
    drains
  }

  def iterator: Iterator [(DocketId, Long)] =
    for {
      (typ, objs) <- pages.iterator
      (obj, gens) <- objs.iterator
      (gen, disks) <- gens.iterator
      (disk, segs) <- disks.iterator
      (seg, bytes) <- segs.iterator
    } yield {
      (DocketId (typ, obj, gen, disk, seg), bytes)
    }

  override def foreach [U] (f: ((DocketId, Long)) => U): Unit =
    for {
      (typ, objs) <- pages
      (obj, gens) <- objs
      (gen, disks) <- gens
      (disk, segs) <- disks
      (seg, bytes) <- segs
    } {
      f (DocketId (typ, obj, gen, disk, seg), bytes)
    }

  override def clone = new AllocationDocket (pages)

  override def toString = s"AllocationDocket(${pages mkString ","})"
}

private object AllocationDocket {

  case class DocketId (typ: TypeId, obj: ObjectId, gen: Long, disk:Int, seg: Int) {

    def matches (typ: TypeId, obj: ObjectId, gens: Set [Long]): Boolean =
      this.typ == typ && this.obj == obj && (gens contains gen)
  }

  val pickler = {
    import DiskPicklers._
    wrap (map (fixedLong, map (fixedLong, map (fixedLong, map (uint, map (int, ulong))))))
    .build (new AllocationDocket (_))
    .inspect (_.pages)
  }}