package com.treode.disk

import java.nio.file.Path

import com.treode.async.{Callback, Scheduler, callback, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

private class DiskDrive (
    val id: Int,
    val path: Path,
    val file: File,
    val config: DiskDriveConfig,
    val logd: LogDispatcher,
    val paged: PageDispatcher) (
        implicit val scheduler: Scheduler) {

  val alloc = new SegmentAllocator (config)
  val logw = new LogWriter (this)
  val pagew = new PageWriter (this)
  var checkpoint: (Int, LogWriter.Meta) = null
  var recovery: (Int, PageWriter.Meta) = null

  def allocated: IntSet = ???

  def init (boot: BootBlock, cb: Callback [Unit]) {
    val latch = Callback.latch (2, cb)
    alloc.init()
    logw.init (latch)
    pagew.init()
    checkpoint (boot.gen)
    checkpoint (boot, latch)
  }

  def checkpoint (gen: Int) {
    require (checkpoint == null)
    checkpoint = (gen, logw.checkpoint (gen))
  }

  def checkpoint (boot: BootBlock, cb: Callback [Unit]) {
    require (checkpoint != null)
    val (gen, log) = checkpoint
    checkpoint = null
    require (gen == boot.gen)
    val latch = Callback.latch (2, cb)
    val superblock = SuperBlock (
        id,
        boot,
        config,
        alloc.checkpoint (gen),
        log,
        pagew.checkpoint (gen, latch))
    val buffer = PagedBuffer (12)
    SuperBlock.pickler.pickle (superblock, buffer)
    val pos = if ((boot.gen & 1) == 0) 0 else SuperBlockBytes
    file.flush (buffer, pos, latch)
  }

  def recover (superblock: SuperBlock) {
    require (recovery == null)
    val gen = superblock.boot.gen
    alloc.recover (gen, superblock.alloc)
    logw.recover (gen, superblock.log)
    recovery = (gen, superblock.pages)
  }

  def recover (pages: PageRegistry, cb: Callback [Unit]) {
    if (recovery != null) {
      val (gen, meta) = recovery
      recovery = null
      pagew.recover (gen, meta, pages, cb)
    } else {
      cb()
    }}

  def engage() {
    logd.engage (logw)
    paged.engage (pagew)
  }

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]): Unit =
    guard (cb) {
      val buf = PagedBuffer (12)
      file.fill (buf, pos.offset, pos.length, callback (cb) { _ =>
        desc.ppag.unpickle (buf)
      })
    }

  def logIterator (records: RecordRegistry, cb: Callback [LogIterator]): Unit =
    logw.iterator (records, cb)

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDrive => path == that.path
      case _ => false
    }

  override def toString = s"DiskDrive($id, $path, $config)"
}
