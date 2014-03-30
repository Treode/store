package com.treode.disk

import scala.collection.immutable.Queue
import com.treode.async.{Async, AsyncImplicits, Callback, Fiber, Latch, Scheduler}
import scala.util.{Failure, Success}

import Async.{async, guard}
import AsyncImplicits._
import Callback.ignore
import PageLedger.Groups

private class Compactor (kit: DisksKit) {
  import kit.{config, disks, releaser, scheduler}

  type DrainReq = Iterator [SegmentPointer]

  val empty = (Set.empty [PageGroup], List.empty [Callback [Unit]])

  val fiber = new Fiber (scheduler)
  var pages: PageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var drainq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [PageGroup], List [Callback [Unit]])]
  var segments = 0
  var cleanreq = false
  var drainreq = Queue.empty [DrainReq]
  var engaged = true

  private def reengage() {
    if (!cleanq.isEmpty) {
      val (typ, obj) = cleanq.head
      cleanq = cleanq.tail
      compactObject (typ, obj)
    } else if (cleanreq) {
      cleanreq = false
      probeForClean()
    } else if (!drainreq.isEmpty) {
      val (first, rest) = drainreq.dequeue
      drainreq = rest
      probeForDrain (first)
    } else if (!drainq.isEmpty) {
      val (typ, obj) = drainq.head
      drainq = drainq.tail
      compactObject (typ, obj)
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

  private def compactObject (typ: TypeId, obj: ObjectId) {
    val (groups, latches) = book (typ, obj)
    book -= ((typ, obj))
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

  private def compact (groups: Groups, segments: Seq [SegmentPointer], cleaning: Boolean): Unit =
    fiber.execute {
      val latch = Latch.unit [Unit] (groups.size, release (segments))
      for ((disk, segs) <- segments groupBy (_.disk))
        disk.compacting (segs)
      for ((id, gs1) <- groups) {
        if (cleaning) {
          if (!(cleanq contains id))
            cleanq += id
          if (drainq contains id)
            drainq -= id
        } else {
          if (!(cleanq contains id) && !(drainq contains id))
            drainq += id
        }
        book.get (id) match {
          case Some ((gs0, ls0)) =>
            book += id -> ((gs0 ++ gs1, latch :: ls0))
          case None =>
            book += id -> ((gs1, latch :: Nil))
        }}}

  def launch (pages: PageRegistry): Async [Unit] =
    fiber.supply {
      this.pages = pages
      reengage()
    }

  def clean(): Unit =
    fiber.execute {
      if (!engaged)
        probeForClean()
      else
        cleanreq = true
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

  def drain (iter: Iterator [SegmentPointer]): Unit =
    fiber.execute {
      if (!engaged)
        probeForDrain (iter)
      else
        drainreq = drainreq.enqueue (iter)
    }}
