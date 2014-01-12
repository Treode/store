package com.treode.store.disk2

import java.util.concurrent.Callable
import scala.reflect.ClassTag
import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Callback, Future, callback, guard}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, unpickle}

private class PageCache {

  class Load (p: Pickler [_], disks: Map [Int, DiskDrive], disk: Int, pos: Long, len: Int)
  extends Callable [Future [Any]] {
    def call(): Future [Any] = {
      val fut = new Future [Any]
      guard (fut) {
        val buf = PagedBuffer (12)
        disks (disk) fill (buf, pos, len, callback (fut) (_ => unpickle (p, buf)))
      }
      fut
    }}

  private val pages = CacheBuilder.newBuilder
      .maximumSize (10000)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [P] (p: Pickler [P], tag: ClassTag [P], disks: Map [Int, DiskDrive],
      disk: Int, pos: Long, len: Int, cb: Callback [P]) {
    guard (cb) {
      pages
          .get ((disk, pos), new Load (p, disks, disk, pos, len))
          .get (callback (cb) (v => tag.runtimeClass.cast (v) .asInstanceOf [P]))
    }}
}
