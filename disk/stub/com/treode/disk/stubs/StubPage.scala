package com.treode.disk.stubs

import com.treode.disk.{ObjectId, PageDescriptor, TypeId}

private case class StubPage (typ: TypeId, obj: ObjectId, group: Array [Byte], data: Array [Byte]) {

  def length = data.length
}

private object StubPage {

  def apply [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): StubPage =
    new StubPage (desc.id, obj, desc.pgrp.toByteArray (group), desc.ppag.toByteArray (page))
}
