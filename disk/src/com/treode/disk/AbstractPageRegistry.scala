package com.treode.disk

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.Async

import AbstractPageRegistry.{PickledHandler, Probe}
import Async.guard

private abstract class AbstractPageRegistry {

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

  def compact (id: TypeId, obj: ObjectId, groups: Set [PageGroup]): Async [Unit] =
    guard {
      get (id) .compact (obj, groups)
    }}

private object AbstractPageRegistry {

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
