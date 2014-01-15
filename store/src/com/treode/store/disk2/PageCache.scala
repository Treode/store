package com.treode.store.disk2

import java.util.concurrent.Callable
import scala.reflect.ClassTag
import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Callback, Future, Scheduler, callback, guard}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, unpickle}

private class PageCache (scheduler: Scheduler) {

  class Load (p: Pickler [_], disks: Map [Int, DiskDrive], pos: Position)
  extends Callable [Future [Any]] {
    def call(): Future [Any] = {
      val fut = new Future [Any] (scheduler)
      guard (fut) {
        val disk = disks (pos.disk)
        val buf = PagedBuffer (12)
        disk.fill (buf, pos.offset, pos.length, callback (fut) { _ =>
          unpickle (p, buf)
        })
      }
      fut
    }}

  private val pages = CacheBuilder.newBuilder
      .maximumSize (10000)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [P] (p: Pickler [P], tag: ClassTag [P], disks: Map [Int, DiskDrive], pos: Position, cb: Callback [P]) {
    guard (cb) {
      pages
          .get ((pos.disk, pos.offset), new Load (p, disks, pos))
          .get (callback (cb) (v => tag.runtimeClass.cast (v) .asInstanceOf [P]))
    }}
}
