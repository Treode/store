package com.treode.disk

import java.util.PriorityQueue
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Latch, callback, continue}

class SegmentCleaner (disks: DiskDrives, pages: PageRegistry) {
  import SegmentCleaner.{Groups, union}
  import disks.{config, releaser}

  val threshold = 0.9

  def compact (groups: Groups, cb: Callback [Unit]) {
    val latch = Latch.unit (groups.size, cb)
    for ((id, gs) <- groups)
      pages.compact (id, gs, latch)
  }

  def probe (iter: Iterator [SegmentPointer], cb: Callback [Seq [(SegmentPointer, PageLedger)]]) {

    val candidates = new PriorityQueue [(Double, SegmentPointer, PageLedger)]
    var seg: SegmentPointer = null
    var bounds: SegmentBounds = null

    val loop = new Callback [PageLedger] {

      var ledger: PageLedger = null

      val pagesProbed = continue (cb) { live: Long =>
        if (live == 0) {
          disks.free (Seq (seg))
        } else {
          val util = live.toDouble / (bounds.limit - bounds.pos).toDouble
          if (util < threshold)
            candidates.add ((1D - util, seg, ledger))
          if (candidates.size >= config.cleaningLoad)
            candidates.remove()
        }
        if (iter.hasNext) {
          seg = iter.next
          bounds = seg.disk.geometry.segmentBounds (seg.num)
          PageLedger.read (seg.disk.file, bounds.pos, this)
        } else {
          cb (candidates.map (v => (v._2, v._3)) .toSeq)
        }}

      def pass (ledger: PageLedger) {
        this.ledger = ledger
        pages.probe (ledger, pagesProbed)
      }

      def fail (t: Throwable) = cb.fail (t)
    }

    if (iter.hasNext) {
      seg = iter.next
      bounds = seg.disk.geometry.segmentBounds (seg.num)
      PageLedger.read (seg.disk.file, bounds.pos, loop)
    } else {
      cb (List.empty)
    }}

  def clean (iter: Iterator [SegmentPointer], cb: Callback [Boolean]) {
    probe (iter, continue (cb) { segments =>
      val groups = union (segments map (_._2.groups))
      compact (groups, callback (cb) { _ =>
        releaser.release (segments map (_._1))
        !groups.isEmpty
      })
    })
  }
}

object SegmentCleaner {

  type Groups = Map [TypeId, Set [PageGroup]]

  def union (maps: Seq [Groups]): Groups = {
    var result = Map.empty [TypeId, Set [PageGroup]]
    for {
      groups <- maps
      (id, gs1) <- groups
    } {
      result.get (id) match {
        case Some (gs0) => result += (id -> (gs0 ++ gs1))
        case None => result += (id -> gs1)
      }}
    result
  }}
