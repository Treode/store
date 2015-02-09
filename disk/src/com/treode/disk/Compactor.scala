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

package com.treode.disk

import scala.collection.immutable.Queue
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber, Latch, Scheduler}
import com.treode.async.implicits._

import Async.{async, guard}
import Callback.ignore
import PageLedger.Groups

private class Compactor (kit: DiskKit) {
  import kit.{config, drives, releaser, scheduler}

  type DrainReq = Iterable [SegmentPointer]

  val fiber = new Fiber
  var pages: PageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var compactq = Set.empty [(TypeId, ObjectId)]
  var drainq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [PageGroup], List [Callback [Unit]])]
  var segments = 0
  var cleanreq = false
  var drainreq = Queue.empty [DrainReq]
  var closed = false
  var engaged = true

  private def reengage() {
    if (closed) {
      ()
    } else if (cleanreq) {
      cleanreq = false
      probeForClean()
    } else if (!drainreq.isEmpty) {
      val (first, rest) = drainreq.dequeue
      drainreq = rest
      probeForDrain (first)
    } else if (!cleanq.isEmpty) {
      compactObject (cleanq.head)
    } else if (!compactq.isEmpty) {
      compactObject (compactq.head)
    } else if (!drainq.isEmpty) {
      compactObject (drainq.head)
    } else {
      book = Map.empty
      engaged = false
    }}

  private def compacted (latches: Seq [Callback [Unit]]): Callback [Unit] = {
    case Success (v) =>
      val cb = Callback.fanout (latches)
      fiber.execute (reengage())
      cb.pass (v)
    case Failure (t) =>
      throw t
  }

  private def compactObject (id: (TypeId, ObjectId)) {
    val (typ, obj) = id
    val (groups, latches) = book (typ, obj)
    cleanq -= id
    compactq -= id
    drainq -= id
    book -= id
    engaged = true
    pages.compact (typ, obj, groups) .run (compacted  (latches))
  }

  private def probed: Callback [Unit] = {
    case Success (v) =>
      fiber.execute (reengage())
    case Failure (t) =>
      throw t
  }

  private def probeForClean(): Unit =
    guard {
      segments = 0
      engaged = true
      for {
        iter <- drives.cleanable()
        (segs, groups) <- pages.probeByUtil (iter, 9000)
      } yield compact (groups, segs, true)
    } run (probed)

  private def probeForDrain (iter: Iterable [SegmentPointer]): Unit =
    guard {
      engaged = true
      for (groups <- pages.probeForDrain (iter))
        yield compact (groups, iter.toSeq, false)
    } run (probed)

  private def release (segments: Seq [SegmentPointer]): Callback [Unit] = {
    case Success (v) =>
      releaser.release (segments foreach (_.free()))
    case Failure (t) =>
      // Exception already reported by compacted callback
  }

  private def compact (id: (TypeId, ObjectId), groups: Set [PageGroup]): Async [Unit] =
    async { cb =>
      book.get (id) match {
        case Some ((groups0, cbs0)) =>
          book += id -> ((groups0 ++ groups, cb :: cbs0))
        case None =>
          book += id -> ((groups, cb :: Nil))
      }}

  private def compact (groups: Groups, segments: Seq [SegmentPointer], cleaning: Boolean): Unit =
    fiber.execute {
      val latch = Latch.unit [Unit] (groups.size, release (segments))
      for ((disk, segs) <- segments groupBy (_.disk))
        disk.compacting (segs)
      for ((id, gs) <- groups) {
        if (cleaning)
          cleanq += id
        else
          drainq += id
        compact (id, gs) run (latch)
      }}

  def launch (pages: PageRegistry): Async [Unit] =
    fiber.supply {
      this.pages = pages
      reengage()
    }

  def tally (segments: Int): Unit =
    fiber.execute {
      this.segments += segments
      if (config.clean (this.segments))
        if (!engaged)
          probeForClean()
        else
          cleanreq = true
    }

  def compact (typ: TypeId, obj: ObjectId): Async [Unit] =
    fiber.async { cb =>
      val id = (typ, obj)
      compactq += id
      compact (id, Set.empty [PageGroup]) run (cb)
      if (!engaged)
        reengage()
    }

  def clean(): Unit =
    fiber.execute {
      if (!engaged)
        probeForClean()
      else
        cleanreq = true
    }

  def drain (iter: Iterable [SegmentPointer]): Unit =
    fiber.execute {
      if (!engaged)
        probeForDrain (iter)
      else
        drainreq = drainreq.enqueue (iter)
    }

  def close(): Async [Unit] =
    fiber.guard {
      closed = true
      pages.close()
    }}
