package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, Fiber, Latch, callback, continue, defer}
import com.treode.pickle.{Pickler, PicklerRegistry}

import CheckpointRegistry.writer
import PicklerRegistry.{Tag, tag}

private class CheckpointRegistry (implicit disks: DiskDrives) {
  import disks.{config}

  private val checkpoints = new ArrayList [Callback [Tag] => Unit]

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any): Unit =
    synchronized {
      checkpoints.add { cb =>
        f (callback (cb) (tag (desc.pblk, desc.id.id, _)))
      }}

  def checkpoint (rootgen: Int, cb: Callback [Position]): Unit =
    defer (cb) {
      synchronized {
        val allWritten = continue (cb) { roots: Seq [Tag] =>
          writer.write (rootgen, roots) .run (cb)
        }
        val oneWritten = Latch.seq (checkpoints.size, allWritten)
        checkpoints foreach (_ (oneWritten))
      }}
}

private object CheckpointRegistry {

  def pager [T] (p: Pickler [T]) = {
    import DiskPicklers._
    PageDescriptor (0x6EC7584D, int, seq (p))
  }

  val writer = pager (PicklerRegistry.pickler [Tag])
}
