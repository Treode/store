package com.treode.disk

import com.treode.async.Async
import com.treode.async.implicits._

import Async.guard
import PageLedger.{Groups, Merger}

private class PageRegistry (kit: DiskKit) extends AbstractPageRegistry {
  import kit.{config, releaser, scheduler}

  def probe (ledger: PageLedger): Async [Long] = {
    val _liveGroups =
      for (((typ, obj), groups) <- ledger.groups.latch.map)
        probe (typ, obj, groups)
    for (liveGroups <- _liveGroups)
      yield ledger.liveBytes (liveGroups)
  }

  def probeByUtil (iter: Iterator [SegmentPointer], threshold: Int):
      Async [(Seq [SegmentPointer], Groups)] = {

    val candidates =
      new MinimumCollector [(SegmentPointer, Groups)] (config.cleaningLoad)

    iter.async.foreach { seg =>
      for {
        ledger <- seg.probe()
        live <- probe (ledger)
      } yield {
        if (live == 0) {
          seg.compacting()
          releaser.release (Seq (seg))
        } else {
          val util =
            ((live.toDouble / (seg.limit - seg.base).toDouble) * 10000D).toInt
          if (util < threshold)
            candidates.add (util, (seg, ledger.groups))
        }}
    } .map { _ =>
      val result = candidates.result
      val segs = result map (_._1)
      val groups = PageLedger.merge (result map (_._2))
      (segs, groups)
    }}

  def probeForDrain (iter: Iterator [SegmentPointer]): Async [Groups] = {

    val merger = new Merger

    iter.async.foreach { seg =>
      for {
        ledger <- seg.probe()
      } yield {
        if (ledger.isEmpty) {
          seg.compacting()
          releaser.release (Seq (seg))
        } else {
          merger.add (ledger.groups)
        }}
    } .map { _ =>
      merger.result
    }}}
