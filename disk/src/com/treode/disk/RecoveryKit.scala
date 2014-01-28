package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async._
import com.treode.buffer.PagedBuffer
import com.treode.pickle.PicklerRegistry

import PicklerRegistry.TaggedFunction

private class RecoveryKit (scheduler: Scheduler, disks: DisksKit) extends Recovery {

  private val records = new RecordRegistry
  private var loaders = PicklerRegistry [TaggedFunction [Callback [Unit], Any]] ()
  private var openers = new ArrayList [Recovery => Any]
  private var closers = new ArrayList [Runnable]

  def open (f: Recovery => Any): Unit = {
    require (openers != null, "Recovery has already begun.")
    openers.add (f)
  }

  def reload [B] (desc: RootDescriptor [B]) (f: (B, Callback [Unit]) => Any) {
    require (loaders != null, "Recovery has already read roots.")
    PicklerRegistry.tupled (loaders, desc.pblk, desc.id.id) (f)
  }

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    records.onReplay (desc) (f)

  def close (f: => Any): Unit = synchronized {
    require (closers != null, "Recovery has already closed.")
    closers.add (toRunnable (f))
  }

  private def open() {
    val fs = synchronized {
      val fs = openers
      openers = null
      fs
    }
    fs foreach (_ (this))
  }

  private def close() {
    val fs = synchronized {
      val fs = closers
      closers = null
      fs
    }
    fs foreach (scheduler.execute (_))
  }

  def recover() {
    open()
    this.loaders = null
    close()
  }

  def recover (meta: RootRegistry.Meta, cb: Callback [Unit]) {

    open()

    val logsReplayed = callback (cb) { _: Unit =>
      close()
    }

    val logsMerged = delay (cb) { iter: ReplayIterator =>
      AsyncIterator.foreach (iter, cb) { case ((time, replay), cb) =>
        guard (cb) (replay())
        cb()
      }}

    val rootsLoaded = delay (cb) { _: Unit =>
      disks.replayIterator (records, logsMerged)
    }

    val loaders = synchronized {
      val ls = this.loaders
      this.loaders = null
      ls
    }

    val buf = PagedBuffer (12)

    val rootsRead = delay (cb) { _: Unit =>
      val roots = DiskPicklers.seq (loaders.pickler) unpickle (buf)
      val latch = Callback.latch (roots.size, rootsLoaded)
      roots foreach (_ (latch))
    }

    disks.fill (buf, meta.pos, rootsRead)
  }}
