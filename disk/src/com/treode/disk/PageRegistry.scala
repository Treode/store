package com.treode.disk

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.{Async, AsyncConversions, Callback, Latch}

import Async.guard
import AsyncConversions._
import PageLedger.{Groups, Merger}
import PageRegistry.{PickledHandler, Probe}

private class PageRegistry (kit: DisksKit) {
  import kit.{config, releaser, scheduler}

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
     }}}
