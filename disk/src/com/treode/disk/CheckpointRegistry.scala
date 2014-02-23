package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Async, AsyncConversions}
import com.treode.pickle.{Pickler, PicklerRegistry}

import Async.{async, guard}
import AsyncConversions._
import CheckpointRegistry.writer
import PicklerRegistry.{Tag, tag}

private class CheckpointRegistry (implicit disks: DiskDrives) {
  import disks.{config}

  private val checkpoints = new ArrayList [Unit => Async [Tag]]

  def checkpoint [B] (desc: RootDescriptor [B]) (f: => Async [B]): Unit =
    checkpoints.add {
      _ => f map (tag (desc.pblk, desc.id.id, _))
    }

  def checkpoint (rootgen: Int): Async [Position] =
    guard {
      for {
        roots <- checkpoints.latch.seq (_())
        pos <- writer.write (rootgen, roots)
      } yield pos
    }}

private object CheckpointRegistry {

  def pager [T] (p: Pickler [T]) = {
    import DiskPicklers._
    PageDescriptor (0x6EC7584D, int, seq (p))
  }

  val writer = pager (PicklerRegistry.pickler [Tag])
}
