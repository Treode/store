package com.treode.disk

import scala.collection.immutable.Queue
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber, Latch, Scheduler}
import com.treode.async.implicits._

import Async.{async, guard}
import Callback.ignore
import PageLedger.Groups

private class Compactor (kit: DisksKit) {
  import kit.{config, disks, releaser, scheduler}

  type DrainReq = Iterator [SegmentPointer]

  val fiber = new Fiber (scheduler)
  var pages: PageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var compactq = Set.empty [(TypeId, ObjectId)]
  var drainq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [PageGroup], List [Callback [Unit]])]
  var segments = 0
  var cleanreq = false
  var drainreq = Queue.empty [DrainReq]
  var engaged = true

  private def reengage() {
    if (cleanreq) {
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
      val cb = Callback.fanout (latches, scheduler)
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
        iter <- disks.cleanable()
        (segs, groups) <- pages.probeByUtil (iter, 9000)
      } yield compact (groups, segs, true)
    } run (probed)

  private def probeForDrain (iter: Iterator [SegmentPointer]): Unit =
    guard {
      engaged = true
      for (groups <- pages.probeForDrain (iter))
        yield compact (groups, iter.toSeq, false)
    } run (probed)

  private def release (segments: Seq [SegmentPointer]): Callback [Unit] = {
    case Success (v) =>
      releaser.release (segments)
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
      for ((id, gs1) <- groups) {
        if (cleaning)
          cleanq += id
        else
          drainq += id
        compact (id, gs1) run (latch)
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

  def drain (iter: Iterator [SegmentPointer]): Unit =
    fiber.execute {
      if (!engaged)
        probeForDrain (iter)
      else
        drainreq = drainreq.enqueue (iter)
    }}
