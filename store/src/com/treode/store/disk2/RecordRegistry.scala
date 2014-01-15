package com.treode.store.disk2

import com.treode.async.{Callback, callback}
import com.treode.async.io.{File, Framer}
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, unpickle}

private class RecordRegistry {

  private val records = new Framer [TypeId, RecordHeader, Unit => Any] (RecordRegistry.framer)

  def read (file: File, pos: Long, buf: PagedBuffer, cb: Callback [records.FileFrame]): Unit =
    records.read (file, pos, buf, cb)

  def register [R] (p: Pickler [R], id: TypeId) (f: R => Any): Unit =
    records.register (p, id) (v => _ => f (v))
}

private object RecordRegistry {

  val framer: Framer.Strategy [TypeId, RecordHeader] =
    new Framer.Strategy [TypeId, RecordHeader] {

    def newEphemeralId = ???

    def isEphemeralId (id: TypeId) = false

    def readHeader (buf: PagedBuffer): (Option [TypeId], RecordHeader) = {
      val hdr = unpickle (RecordHeader.pickle, buf)
      hdr match {
        case RecordHeader.Entry (time, id) => (Some (id), hdr)
        case _ => (None, hdr)
      }}

    def writeHeader (hdr: RecordHeader, buf: PagedBuffer) {
      pickle (RecordHeader.pickle, hdr, buf)
    }}}
