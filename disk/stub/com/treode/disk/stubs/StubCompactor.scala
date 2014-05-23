package com.treode.disk.stubs

import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Fiber, Callback, Latch, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.{async, guard}
import PageLedger.Groups

private class StubCompactor (
    releaser: EpochReleaser
 ) (implicit
     random: Random,
     scheduler: Scheduler,
     disk: StubDiskDrive,
     config: StubDiskConfig
) {

  val fiber = new Fiber
  var pages: StubPageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var compactq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [PageGroup], List [Callback [Unit]])]
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
      entries = 0
      for {
        (groups, segments) <- pages.probe (disk.cleanable())
      } yield compact (groups, segments)
    } run (probed)

  private def release (segments: Seq [Long]): Callback [Unit] = {
    case Success (v) =>
      releaser.release (disk.free (segments))
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

  private def compact (groups: Groups, segments: Seq [Long]): Unit =
    fiber.execute {
      val latch = Latch.unit [Unit] (groups.size, release (segments))
      for ((id, gs) <- groups) {
        cleanq += id
        compact (id, gs) run (latch)
      }}

  def launch (pages: StubPageRegistry): Unit =
    fiber.execute {
      this.pages = pages
      reengage()
    }

  def tally(): Unit =
    fiber.execute {
      entries += 1
      if (config.compact (entries))
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
    }}
