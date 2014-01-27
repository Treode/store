package com.treode.disk

import com.treode.async.{Callback, callback}
import com.treode.async.io.{File, Framer}
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.pickle.Pickler

private class RecordRegistry {

  private val records = new Framer [TypeId, RecordHeader, Unit => Any] (RecordRegistry.framer)

  def onReplay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    records.register (desc.prec, desc.id) (v => _ => f (v))

  def read (file: File, pos: Long, buf: PagedBuffer, cb: Callback [records.FileFrame]): Unit =
    records.read (file, pos, buf, cb)
}

private object RecordRegistry {

  val framer: Framer.Strategy [TypeId, RecordHeader] =
    new Framer.Strategy [TypeId, RecordHeader] {

      def newEphemeralId = ???

      def isEphemeralId (id: TypeId) = false

      def readHeader (in: Input): (Option [TypeId], RecordHeader) = {
        val hdr = RecordHeader.pickler.unpickle (in)
        hdr match {
          case RecordHeader.Entry (time, id) => (Some (id), hdr)
          case _ => (None, hdr)
        }}

      def writeHeader (hdr: RecordHeader, out: Output) {
        RecordHeader.pickler.pickle (hdr, out)
      }}}
