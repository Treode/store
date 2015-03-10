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

import com.treode.disk.{ObjectId, PickledPage, TypeId}

import PageTally.TallyId

/** A convenient facade for a `Map [(TypeId, ObjectId, Long), Long]`.
  *
  * When flushing a batch of writes, the `PageWriter` uses this class to track the bytes
  * allocated to each generation of each object. After flushing the batch, the PageWriter hands
  * this summary to the SegmentLedger, which adds the new allocations to its running totals.
  */
private class PageTally private (private var pages: Map [TallyId, Long])
extends Iterable [(TallyId, Long)] with Traversable [(TallyId, Long)] {

  def this() = this (Map.empty.withDefaultValue (0))

  def alloc (page: TallyId, bytes: Long): Unit = {
    if (bytes == 0) return
    pages += page -> (pages (page) + bytes)
  }

  def alloc (typ: TypeId, obj: ObjectId, gen: Long, bytes: Long): Unit =
    alloc (new TallyId (typ, obj, gen), bytes)

  def alloc (page: PickledPage, bytes: Long): Unit =
    alloc (page.typ, page.obj, page.gen, bytes)

  def iterator = pages.iterator

  override def foreach [U] (f: ((TallyId, Long)) => U) = pages.foreach (f)

  override def toString = s"Tally(${pages mkString ","})"
}

private object PageTally {

  case class TallyId (typ: TypeId, obj: ObjectId, gen: Long)

  object TallyId {

    val pickler = {
      import DiskPicklers._
      wrap (typeId, objectId, ulong)
      .build ((TallyId.apply _).tupled)
      .inspect (v => (v.typ, v.obj, v.gen))
    }}

  val pickler = {
    import DiskPicklers._
    wrap (map (TallyId.pickler, long))
    .build (new PageTally (_))
    .inspect (_.pages)
  }}
