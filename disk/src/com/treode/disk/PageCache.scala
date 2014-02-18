package com.treode.disk

import java.util.concurrent.Callable
import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Async, Future}

import Async.guard

private class PageCache (disks: DiskDrives) {
  import disks.scheduler

  class Load (desc: PageDescriptor [_, Any], pos: Position)
  extends Callable [Future [Any]] {
    def call(): Future [Any] =
      disks.fetch (desc, pos) .toFuture
  }

  private val pages = CacheBuilder.newBuilder
      .maximumSize (10000)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      pages
          .get ((pos.disk, pos.offset), new Load (desc.asInstanceOf [PageDescriptor [_, Any]], pos))
          .get()
          .map (v => desc.tpag.runtimeClass.cast (v) .asInstanceOf [P])
  }}
