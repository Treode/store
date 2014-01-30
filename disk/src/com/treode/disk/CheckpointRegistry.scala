package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, callback, delay}
import com.treode.pickle.{Pickler, PicklerRegistry}

import PicklerRegistry.{Tag, tag}

private class CheckpointRegistry (implicit disks: Disks) {

  private val checkpoints = new ArrayList [Callback [Tag] => Unit]

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any): Unit =
    synchronized {
      checkpoints.add { cb =>
        f (callback (cb) (tag (desc.pblk, desc.id.id, _)))
      }}

  def checkpoint (gen: Int, cb: Callback [Position]) = synchronized {
    val allWritten = delay (cb) { roots: Seq [Tag] =>
      CheckpointRegistry.writer.write (gen, roots, cb)
    }
    val oneWritten = Callback.seq (checkpoints.size, allWritten)
    checkpoints foreach (_ (oneWritten))
  }}

private object CheckpointRegistry {

  def pager [T] (p: Pickler [T]) = {
    import DiskPicklers._
    new PageDescriptor (0x6EC7584D, int, seq (p))
  }

  val writer = pager (PicklerRegistry.pickler [Tag])
}
