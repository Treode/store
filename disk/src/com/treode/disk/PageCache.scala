package com.treode.disk

import java.util.concurrent.Callable
import scala.util.Success

import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Async, Future}

import Async.guard

private class PageCache (kit: DiskKit) {
  import kit.config.pageCacheEntries
  import kit.drives

  class Load (desc: PageDescriptor [_, Any], pos: Position)
  extends Callable [Future [Any]] {
    def call(): Future [Any] =
      drives.fetch (desc, pos) .toFuture
  }

  private val pages = CacheBuilder.newBuilder
      .maximumSize (pageCacheEntries)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      pages
          .get ((pos.disk, pos.offset), new Load (desc.asInstanceOf [PageDescriptor [_, Any]], pos))
          .map (v => desc.tpag.runtimeClass.cast (v) .asInstanceOf [P])
    }

  def write [P] (pos: Position, page: P): Unit = {
    val future = new Future [Any]
    future (Success (page))
    pages.put ((pos.disk, pos.offset), future)
  }}
