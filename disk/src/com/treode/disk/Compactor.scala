package com.treode.disk

import scala.collection.immutable.Queue
import com.treode.async.{Async, AsyncConversions, Callback, Fiber, Latch, Scheduler}
import scala.util.{Failure, Success}

import Async.{async, guard}
import AsyncConversions._
import Callback.ignore
import PageLedger.Groups

private class Compactor (kit: DisksKit) {
  import kit.{config, disks, releaser, scheduler}

  val empty = (Set.empty [PageGroup], List.empty [Callback [Unit]])

  val fiber = new Fiber (scheduler)
  var pages: PageRegistry = null
  var cleanq = Set.empty [(TypeId, ObjectId)]
  var drainq = Set.empty [(TypeId, ObjectId)]
  var book = Map.empty [(TypeId, ObjectId), (Set [PageGroup], List [Callback [Unit]])]
  var segments = 0
  var engaged = true
  var cleanreq = true

  private def reengage() {
    if (!cleanq.isEmpty) {
      val (typ, obj) = cleanq.head
      cleanq = cleanq.tail
      compact (typ, obj)
    } else if (config.clean (segments)) {
      engaged == false
      clean()
    } else if (!drainq.isEmpty) {
      val (typ, obj) = drainq.head
      drainq = drainq.tail
      cleanreq = false
      compact (typ, obj)
    } else {
      book = Map.empty
      engaged = false
      cleanreq = false
    }}

  private def compacted (latches: Seq [Callback [Unit]]): Callback [Unit] = {
    case Success (v) =>
      val cb = Callback.fanout (latches, scheduler)
      fiber.execute (reengage())
      cb.pass (v)
    case Failure (t) =>
      throw t
  }

  private def compact (typ: TypeId, obj: ObjectId) {
    val (groups, latches) = book (typ, obj)
    book -= ((typ, obj))
    engaged = true
    pages.compact (typ, obj, groups) .run (compacted  (latches))
  }

  private def release (segments: Seq [SegmentPointer]): Callback [Unit] = {
    case Success (v) =>
      releaser.release (segments)
    case Failure (t) =>
      // Exception already reported by compacted callback
  }

  private def compact (groups: Groups, segments: Seq [SegmentPointer], cleaning: Boolean): Unit =
    fiber.execute {
      val latch = Latch.unit [Unit] (groups.size, release (segments))
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
        }}
      if (!engaged)
        reengage()
    }

  def launch (pages: PageRegistry): Async [Unit] =
    fiber.supply {
      this.pages = pages
      reengage()
    }

  def clean(): Unit =
    guard {
      cleanreq = true
      for {
        iter <- disks.cleanable()
        (segs, groups) <- pages.probeByUtil (iter, 9000)
      } yield compact (groups, segs, true)
    } run (ignore)

  def tally (segments: Int): Unit =
    fiber.execute {
      this.segments += segments
      if (!cleanreq && config.clean (this.segments))
        clean()
    }

  def drain (iter: Iterator [SegmentPointer]): Unit =
    guard {
      for (groups <- pages.probeForDrain (iter))
        yield compact (groups, iter.toSeq, false)
    } run (ignore)
}
