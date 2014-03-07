package com.treode.disk

import scala.collection.immutable.Queue
import com.treode.async.{Async, Callback, Fiber, Latch, Scheduler}

import Async.async
import PageLedger.Groups

private class Compactor (disks: DiskDrives) {
  import disks.{config, releaser, scheduler}

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

  private def compacted (latches: Seq [Callback [Unit]]): Callback [Unit] =
    new Callback [Unit] {

      val cb = Callback.fanout (latches, scheduler)

      def pass (v: Unit) {
        fiber.execute (reengage())
        cb.pass (v)
      }

      def fail (t: Throwable) {
        disks.panic (t)
        cb.fail (t)
      }}

  private def compact (typ: TypeId, obj: ObjectId) {
    val (groups, latches) = book (typ, obj)
    book -= ((typ, obj))
    engaged = true
    pages.compact (typ, obj, groups) .run (compacted  (latches))
  }

  private def release (segments: Seq [SegmentPointer]): Callback [Unit] =
    new Callback [Unit] {

      def pass (v: Unit) {
        releaser.release (segments)
      }

      def fail (t: Throwable) {
        // Exception already reported by compacted callback
      }}

  private def compact (groups: Groups, segments: Seq [SegmentPointer], cleaning: Boolean): Unit =
    fiber.execute {
      val latch = Latch.unit (groups.size, release (segments))
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

  private def clean() {
    cleanreq = true
    val x = for {
      iter <- disks.cleanable()
      (segs, groups) <- pages.probeByUtil (iter, 0.9)
    } yield compact (groups, segs, true)
    x run (disks.panic)
  }

  def launch (pages: PageRegistry): Unit =
    fiber.execute {
      this.pages = pages
      reengage()
    }

  def tally (segments: Int): Unit =
    fiber.execute {
      this.segments += segments
      if (!cleanreq && config.clean (this.segments))
        clean()
    }

  def drain (iter: Iterator [SegmentPointer]) {
    val x = for {
      groups <- pages.probeForDrain (iter)
    } yield compact (groups, iter.toSeq, false)
    x run (disks.panic)
  }}
