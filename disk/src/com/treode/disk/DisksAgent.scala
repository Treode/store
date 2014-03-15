package com.treode.disk

import com.treode.async.Async

import Async.async

private class DisksAgent (kit: DisksKit) extends Disks {
  import kit.{disks, logd, paged, releaser}

  val cache = new PageCache (disks)

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async (cb => logd.send (PickledRecord (desc, entry, cb)))

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    cache.read (desc, pos)

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position] =
    async (cb => paged.send (PickledPage (desc, obj, group, page, cb)))

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}
