package com.treode.disk

import com.treode.async.{Async, Callback, Fiber}
import com.treode.async.implicits._

import Async.guard
import Callback.fanout
import PageLedger.{Groups, Merger}

private class PageRegistry (kit: DiskKit) extends AbstractPageRegistry {
  import kit.{config, releaser, scheduler}

  private val fiber = new Fiber
  private var closereqs = List.empty [Callback [Unit]]
  private var closed = false
  private var engaged = false

  def close(): Async [Unit] =
    fiber.async { cb =>
      closed = true
      if (engaged)
        closereqs ::= cb
      else
        scheduler.pass (cb, ())
    }

  def disengage(): Unit =
    fiber.execute {
      engaged = false
      if (!closereqs.isEmpty) {
        val cb = fanout (closereqs)
        closereqs = Nil
        scheduler.pass (cb, ())
      }}

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
    engaged = true

    iter.async.whilst (_ => !closed) { seg =>
      for {
        ledger <- seg.probe()
        live <- probe (ledger)
      } yield {
        if (live == 0) {
          seg.compacting()
          releaser.release (seg.free())
        } else {
          val util =
            ((live.toDouble / (seg.limit - seg.base).toDouble) * 10000D).toInt
          if (util < threshold)
            candidates.add (util, (seg, ledger.groups))
        }}
    } .map { _ =>
      disengage()
      val result = candidates.result
      val segs = result map (_._1)
      val groups = PageLedger.merge (result map (_._2))
      (segs, groups)
    }}

  def probeForDrain (iter: Iterator [SegmentPointer]): Async [Groups] = {

    val merger = new Merger
    engaged = true

    iter.async.whilst (_ => !closed) { seg =>
      for {
        ledger <- seg.probe()
      } yield {
        if (ledger.isEmpty) {
          seg.compacting()
          releaser.release (seg.free())
        } else {
          merger.add (ledger.groups)
        }}
    } .map { _ =>
      disengage()
      merger.result
    }}}
