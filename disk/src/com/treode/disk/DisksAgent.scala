package com.treode.disk

import java.util.concurrent.atomic.AtomicLong
import com.treode.async.Async

import Async.{async, guard}

private class DisksAgent (val kit: DisksKit) extends Disks {
  import kit.{compactor, disks, logd, paged, releaser}
  import kit.config.{maximumPageBytes, maximumRecordBytes}

  val cache = new PageCache (disks)

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async { cb =>
      val _entry = PickledRecord (desc, entry, cb)
      if (_entry.byteSize > maximumRecordBytes)
        throw new OversizedRecordException (maximumRecordBytes, _entry.byteSize)
      logd.send (_entry)
    }

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    cache.read (desc, pos)

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position] =
    async [Position] { cb =>
      val _page = PickledPage (desc, obj, group, page, cb)
      if (_page.byteSize > maximumPageBytes)
        throw new OversizedPageException (maximumPageBytes, _page.byteSize)
      paged.send (_page)
    } .map { pos =>
      cache.write (pos, page)
      pos
    }

  def compact (desc: PageDescriptor [_, _], obj: ObjectId): Async [Unit] =
    compactor.compact (desc.id, obj)

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}
