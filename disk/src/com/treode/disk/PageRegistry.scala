package com.treode.disk

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.{Async, AsyncConversions, Callback, Latch}

import Async.guard
import AsyncConversions._
import PageLedger.{Groups, Merger}
import PageRegistry.{PickledHandler, Probe, chooseByMargin}

private class PageRegistry (disks: DiskDrives) {
  import disks.{config, releaser, scheduler}

  val handlers = new ConcurrentHashMap [TypeId, PickledHandler]

  def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]) {
    val h0 = handlers.putIfAbsent (desc.id, PickledHandler (desc, handler))
    require (h0 == null, f"PageHandler ${desc.id.id}%X already registered")
  }

  def get (id: TypeId): PickledHandler = {
    val h = handlers.get (id)
    require (h != null, f"PageHandler ${id.id}%X not registered")
    h
  }

  def probe (typ: TypeId, obj: ObjectId, groups: Set [PageGroup]): Async [Probe] =
    guard {
      get (typ) .probe (obj, groups)
    }

  def probe (ledger: PageLedger): Async [Long] = {
    val _liveGroups = ledger.groups.latch.map {
      case ((typ, obj), groups) =>
        probe (typ, obj, groups)
    }
    for (liveGroups <- _liveGroups)
      yield ledger.liveBytes (liveGroups)
  }

  def probeByUtil (iter: Iterator [SegmentPointer], threshold: Double):
      Async [(Seq [SegmentPointer], Groups)] = {

    val candidates =
      new MinimumCollector [(SegmentPointer, Groups)] (config.cleaningLoad)

    iter.async.foreach { seg =>
      for {
        ledger <- seg.probe()
        live <- probe (ledger)
      } yield {
        if (live == 0) {
          releaser.release (Seq (seg))
        } else {
          val util =
            ((live.toDouble / (seg.limit - seg.pos).toDouble) * 10000D).toInt
          if (util < threshold)
            candidates.add (util, (seg, ledger.groups))
        }}
    } .map { _ =>
      val result = candidates.result
      val segs = result map (_._1)
      val groups = PageLedger.merge (result map (_._2))
      (segs, groups)
    }}

  def probeByMargin (iter: Iterator [SegmentPointer], threshold: Double):
      Async [(Seq [SegmentPointer], Groups)] = {

    val candidates = Seq.newBuilder [(Int, SegmentPointer, Groups)]

    iter.async.foreach { seg =>
      for {
        ledger <- seg.probe()
        live <- probe (ledger)
      } yield {
        if (live == 0) {
          releaser.release (Seq (seg))
        } else {
          val util =
            ((live.toDouble / (seg.limit - seg.pos).toDouble) * 10000D).toInt
          if (util < threshold)
            candidates += ((util, seg, ledger.groups))
        }}
    } .map { _ =>
      chooseByMargin (candidates.result, config.cleaningLoad)
    }}

  def probeForDrain (iter: Iterator [SegmentPointer]): Async [Groups] = {

    val merger = new Merger

    iter.async.foreach { seg =>
      for {
        ledger <- seg.probe()
      } yield {
        merger.add (ledger.groups)
      }
    } .map { _ =>
      merger.result
    }}

  def compact (id: TypeId, obj: ObjectId, groups: Set [PageGroup]): Async [Unit] =
    guard {
      get (id) .compact (obj, groups)
    }}

private object PageRegistry {

  type Probe = ((TypeId, ObjectId), Set [PageGroup])

  trait PickledHandler {

    def probe (obj: ObjectId, groups: Set [PageGroup]): Async [Probe]
    def compact (obj: ObjectId, groups: Set [PageGroup]): Async [Unit]
  }

  object PickledHandler {

    def apply [G] (desc: PageDescriptor [G, _], handler: PageHandler [G]): PickledHandler =
      new PickledHandler {

        def probe (obj: ObjectId, groups: Set [PageGroup]): Async [Probe] = {
          for (live <- handler.probe (obj, groups map (_.unpickle (desc.pgrp))))
            yield ((desc.id, obj), live map (PageGroup (desc.pgrp, _)))
        }

        def compact (obj: ObjectId, groups: Set [PageGroup]): Async [Unit] =
          handler.compact (obj, groups map (_.unpickle (desc.pgrp)))
     }}

  def set (groups: Groups): Set [(TypeId, ObjectId, PageGroup)] = {
    val builder = Set.newBuilder [(TypeId, ObjectId, PageGroup)]
    for (((typ, obj), gs) <- groups; g <- gs)
      builder += ((typ, obj, g))
    builder.result
  }

  def margin (chosen: Set [(TypeId, ObjectId, PageGroup)], groups: Groups): Int = {
    var count = 0
    for (((typ, obj), gs) <-  groups; g <- gs)
      if (!(chosen contains (typ, obj, g)))
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
