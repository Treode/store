package com.treode.disk

import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, PicklerRegistry}

import PicklerRegistry.FunctionTag

private class RecordRegistry {

  private val records = PicklerRegistry [FunctionTag [Unit, Any]] ()

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    PicklerRegistry.delayed (records, desc.prec, desc.id.id) (f)

  def read (id: Long, buf: PagedBuffer, len: Int): Unit => Any =
    records.unpickle (id, buf, len)
}

private object RecordRegistry {

  def frame [R] (desc: RecordDescriptor [R], time: Long, entry: R, buf: PagedBuffer): Unit = {
    import DiskPicklers.tuple
    import RecordHeader.{Entry, pickler}
    tuple (pickler, desc.prec) .frame ((Entry (time, desc.id), entry), buf)
  }}
