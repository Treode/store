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

package com.treode.disk.stubs

import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Fiber, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.{async, guard}
import Callback.ignore
import PageLedger.Groups

private class StubCompactor (
    releaser: EpochReleaser
 ) (implicit
     random: Random,
     scheduler: Scheduler,
     disk: StubDiskDrive,
     config: StubDisk.Config
) {

  val fiber = new Fiber
  var pages: StubPageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var compactq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [GroupId], List [Callback [Unit]])]
  var cleanreq = false
  var entries = 0
  var engaged = true

  private def reengage() {
    if (cleanreq) {
      cleanreq = false
      probeForClean()
    } else if (!cleanq.isEmpty) {
      compactObject (cleanq.head)
    } else if (!compactq.isEmpty) {
      compactObject (compactq.head)
    } else {
      book = Map.empty
      entries = 0
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
      engaged = true
      for {
        (groups, segments) <- pages.probe (disk.cleanable())
      } yield compact (groups, segments)
    } run (probed)

  private def compact (id: (TypeId, ObjectId), groups: Set [GroupId]): Async [Unit] =
    async { cb =>
      book.get (id) match {
        case Some ((groups0, cbs0)) =>
          book += id -> ((groups0 ++ groups, cb :: cbs0))
        case None =>
          book += id -> ((groups, cb :: Nil))
      }}

  private def compact (groups: Groups, segments: Seq [Long]): Unit =
    fiber.execute {
      (for ((id, gs) <- groups.latch) {
        cleanq += id
        compact (id, gs)
      })
      .map (_ => releaser.release (disk.free (segments)))
      .run (ignore)
    }

  def launch (pages: StubPageRegistry): Unit =
    fiber.execute {
      this.pages = pages
      reengage()
    }

  def tally(): Unit =
    fiber.execute {
      entries += 1
      if (!engaged && config.compact (entries))
        probeForClean()
    }

  def compact (typ: TypeId, obj: ObjectId): Async [Unit] =
    fiber.async { cb =>
      val id = (typ, obj)
      compactq += id
      compact (id, Set.empty [GroupId]) run (cb)
      if (!engaged)
        reengage()
    }}
