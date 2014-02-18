package com.treode.disk

import scala.reflect.ClassTag
import com.treode.async.Async
import com.treode.pickle.Pickler

class PageDescriptor [G, P] private (
    val id: TypeId,
    val pgrp: Pickler [G],
    val ppag: Pickler [P]) (
        implicit val tpag: ClassTag [P]) {

  def read (reload: Reload, pos: Position): Async [P] =
    reload.read (this, pos)

  def read (launch: Launch, pos: Position): Async [P] =
    launch.read (this, pos)

  def handle (handler: PageHandler [G]) (implicit launch: Launch): Unit =
    launch.handle (this, handler)

  def read (pos: Position) (implicit disks: Disks): Async [P] =
    disks.read (this, pos)

  def write (group: G, page: P) (implicit disks: Disks): Async [Position] =
    disks.write (this, group, page)

  override def toString = s"PageDescriptor($id)"
}

object PageDescriptor {

  def apply [G, P] (id: TypeId, pgrp: Pickler [G], ppag: Pickler [P]) (
      implicit tpag: ClassTag [P]): PageDescriptor [G, P] =
    new PageDescriptor (id, pgrp, ppag)
}
