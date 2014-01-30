package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, callback, delay}
import com.treode.pickle.{Pickler, PicklerRegistry}

import PicklerRegistry.Tagger

private class CheckpointRegistry (implicit disks: Disks) {

  private val pager = CheckpointRegistry.pager (PicklerRegistry.pickler)

  private val checkpoints = new ArrayList [Callback [Tagger] => Unit]

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any): Unit =
    synchronized {
      checkpoints.add { cb =>
        f (callback (cb) { root =>
          PicklerRegistry.tagger (desc.pblk, desc.id.id, root)
        })
      }}

  def checkpoint (gen: Int, cb: Callback [Position]) = synchronized {
    val allWritten = delay (cb) { roots: Seq [Tagger] =>
      pager.write (gen, roots, cb)
    }
    val oneWritten = Callback.collect (checkpoints.size, allWritten)
    checkpoints foreach (_ (oneWritten))
  }}

private object CheckpointRegistry {

  def pager [T] (p: Pickler [T]) = {
    import DiskPicklers._
    new PageDescriptor (0x6EC7584D, int, seq (p))
  }}
