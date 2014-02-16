package com.treode.disk

import java.util.concurrent.ConcurrentHashMap

import com.treode.async._

import PageLedger.{Groups, Merger}
import PageRegistry.{Handler, chooseByMargin}

private class PageRegistry (disks: DiskDrives) {
  import disks.{config, releaser, scheduler}

  val handlers = new ConcurrentHashMap [TypeId, Handler]

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]) {
    val h0 = handlers.putIfAbsent (desc.id, Handler (desc, handler))
    require (h0 == null, f"PageHandler ${desc.id.id}%X already registered")
  }

  def get (id: TypeId): Handler = {
    val h = handlers.get (id)
    require (h != null, f"PageHandler ${id.id}%X not registered")
    h
  }

  def probe (id: TypeId, groups: Set [PageGroup], cb: Callback [(TypeId, Set [PageGroup])]): Unit =
    defer (cb) {
      get (id) .probe (groups, cb)
    }

  def probe (ledger: PageLedger, cb: Callback [Long]): Unit =
    defer (cb) {
      val pagesProbed = callback (cb) { liveGroups: Map [TypeId, Set [PageGroup]] =>
        var liveBytes = 0L
        for {
          (id, pageGroups) <- liveGroups
          group <- pageGroups
        } liveBytes += ledger.get (id, group)
        liveBytes
      }
      val groupsByType = ledger.groups
      val latch = Latch.map (groupsByType.size, pagesProbed)
      for ((id, groups) <- groupsByType)
        probe (id, groups, latch)
    }

  def probeByUtil (
      iter: Iterator [SegmentPointer],
      threshold: Double,
      cb: Callback [(Seq [SegmentPointer], Groups)]) {

    val candidates =
      new MinimumCollector [(SegmentPointer, Groups)] (config.cleaningLoad)

    val allProbed = callback (cb) { _: Unit =>
      val result = candidates.result
      val segs = result map (_._1)
      val groups = PageLedger.merge (result map (_._2))
      (segs, groups)
    }

    iter.async.foreach.cb { case (seg, cb) =>
      val oneRead = continue (cb) { ledger: PageLedger =>
        val oneProbed = callback (cb) { live: Long =>
          if (live == 0) {
            releaser.release (Seq (seg))
          } else {
            val util =
              ((live.toDouble / (seg.limit - seg.pos).toDouble) * 10000D).toInt
            if (util < threshold)
              candidates.add (util, (seg, ledger.groups))
          }}
        probe (ledger, oneProbed)
      }
      seg.probe (oneRead)
    } .run (allProbed)
  }

  def probeByMargin (
      iter: Iterator [SegmentPointer],
      threshold: Double,
      cb: Callback [(Seq [SegmentPointer], Groups)]) {

    val candidates = Seq.newBuilder [(Int, SegmentPointer, Groups)]

    val allProbed = callback (cb) { _: Unit =>
      chooseByMargin (candidates.result, config.cleaningLoad)
    }

    iter.async.foreach.cb { case (seg, cb) =>
      val oneRead = continue (cb) { ledger: PageLedger =>
        val oneProbed = callback (cb) { live: Long =>
          if (live == 0) {
            releaser.release (Seq (seg))
          } else {
            val util =
              ((live.toDouble / (seg.limit - seg.pos).toDouble) * 10000D).toInt
            if (util < threshold)
              candidates += ((util, seg, ledger.groups))
          }}
        probe (ledger, oneProbed)
      }
      seg.probe (oneRead)
    } .run (allProbed)
  }

  def probeForDrain (iter: Iterator [SegmentPointer], cb: Callback [Groups]) {

    val merger = new Merger

    val allRead = callback (cb) { _: Unit =>
      merger.result
    }

    iter.async.foreach.cb { case (seg, cb) =>
      val oneRead = callback (cb) { ledger: PageLedger =>
        merger.add (ledger.groups)
      }
      seg.probe (oneRead)
    } .run (allRead)
  }

  def compact (id: TypeId, groups: Set [PageGroup], cb: Callback [Unit]): Unit =
    defer (cb) {
      get (id) .compact (groups, cb)
    }}

private object PageRegistry {

  trait Handler {

    def probe (groups: Set [PageGroup], cb: Callback [(TypeId, Set [PageGroup])])
    def compact (groups: Set [PageGroup], cb: Callback [Unit])
  }

  object Handler {

    def apply [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): Handler =
      new Handler {

        def probe (groups: Set [PageGroup], cb: Callback [(TypeId, Set [PageGroup])]) {
          handler.probe (groups map (_.unpickle (desc.pgrp)), callback (cb) { live =>
            (desc.id, live map (PageGroup (desc.pgrp, _)))
          })
        }

        def compact (groups: Set [PageGroup], cb: Callback [Unit]): Unit =
          handler.compact (groups map (_.unpickle (desc.pgrp)), cb)
     }}

  def set (groups: Groups): Set [(TypeId, PageGroup)] = {
    val builder = Set.newBuilder [(TypeId, PageGroup)]
    for ((id, gs) <- groups; g <- gs)
      builder += ((id, g))
    builder.result
  }

  def margin (chosen: Set [(TypeId, PageGroup)], groups: Groups): Int = {
    var count = 0
    for ((id, gs) <-  groups; g <- gs)
      if (!(chosen contains (id, g)))
        count += 1
    count
  }

  def chooseByMargin (report: Seq [(Int, SegmentPointer, Groups)], load: Int) = {
    val minByUtil = report.minBy (_._1)
    val chosenGroups = set (minByUtil._3)
    val minByMargin = new MinimumCollector [(SegmentPointer, Groups)] (load)
    for ((util, seg, groups) <- report)
      minByMargin.add (margin (chosenGroups, groups), (seg, groups))
    val result = minByMargin.result
    val segs = result map (_._1)
    val groups = PageLedger.merge (result map (_._2))
    (segs, groups)
  }}
