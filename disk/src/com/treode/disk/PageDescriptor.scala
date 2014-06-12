package com.treode.disk

import scala.reflect.ClassTag
import com.treode.async.Async
import com.treode.pickle.Pickler

class PageDescriptor [G, P] private (
    val id: TypeId,
    val pgrp: Pickler [G],
    val ppag: Pickler [P]) (
        implicit val tpag: ClassTag [P]) {

  def handle (handler: PageHandler [G]) (implicit launch: Disk.Launch): Unit =
    launch.handle (this, handler)

  def read (pos: Position) (implicit disk: Disk): Async [P] =
    disk.read (this, pos)

  def write (obj: ObjectId, group: G, page: P) (implicit disk: Disk): Async [Position] =
    disk.write (this, obj, group, page)

  def compact (obj: ObjectId) (implicit disk: Disk): Async [Unit] =
    disk.compact (this, obj)

  override def toString = s"PageDescriptor($id)"
}

object PageDescriptor {

  def apply [G, P] (id: TypeId, pgrp: Pickler [G], ppag: Pickler [P]) (
      implicit tpag: ClassTag [P]): PageDescriptor [G, P] =
    new PageDescriptor (id, pgrp, ppag)
}
