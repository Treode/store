package com.treode.disk.stubs

import com.treode.disk.{RecordDescriptor, TypeId}

private case class StubRecord (typ: TypeId, data: Array [Byte])

private object StubRecord {

  def apply [R] (desc: RecordDescriptor [R], entry: R): StubRecord =
    new StubRecord (desc.id, desc.prec.toByteArray (entry))
}
