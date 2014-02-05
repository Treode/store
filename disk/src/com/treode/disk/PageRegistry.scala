package com.treode.disk

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.{Callback, callback, guard}

import PageRegistry.Handler

private class PageRegistry {

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
    guard (cb) {
      get (id) .probe (groups, cb)
    }

  def probe (ledger: PageLedger, cb: Callback [Long]): Unit =
    guard (cb) {
      val pagesProbed = callback (cb) { liveGroups: Map [TypeId, Set [PageGroup]] =>
        var liveBytes = 0L
        for {
          (id, pageGroups) <- liveGroups
          group <- pageGroups
        } liveBytes += ledger.get (id, group)
        liveBytes
      }
      val groupsByType = ledger.groups
      val latch = Callback.map (groupsByType.size, pagesProbed)
      for ((id, groups) <- groupsByType)
        probe (id, groups, latch)
    }

  def compact (id: TypeId, groups: Set [PageGroup], cb: Callback [Unit]): Unit =
    guard (cb) {
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
     }}}
