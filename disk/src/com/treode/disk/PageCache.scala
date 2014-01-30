package com.treode.disk

import java.util.concurrent.Callable

import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Callback, Future, Scheduler, callback, guard}

private class PageCache (disks: DiskDrives) (implicit scheduler: Scheduler) {

  class Load (desc: PageDescriptor [_, _], pos: Position)
  extends Callable [Future [Any]] {
    def call(): Future [Any] = {
      val fut = new Future [Any] (scheduler)
      guard (fut) (disks.fetch (desc, pos, fut))
      fut
    }}

  private val pages = CacheBuilder.newBuilder
      .maximumSize (10000)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]) {
    guard (cb) {
      pages
          .get ((pos.disk, pos.offset), new Load (desc, pos))
          .get (callback (cb) (v => desc.tpag.runtimeClass.cast (v) .asInstanceOf [P]))
    }}
}
