package com.treode.disk

import java.nio.file.Path
import scala.language.postfixOps

import com.treode.async.{Callback, Scheduler, callback, delay, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

private class DiskDrive (
    val id: Int,
    val path: Path,
    val file: File,
    val config: DiskDriveConfig,
    val alloc: SegmentAllocator,
    val logw: LogWriter,
    val pagew: PageWriter) (
        implicit val scheduler: Scheduler) {

  var checkpoint: (Int, LogWriter.Meta) = null

  def allocated: IntSet = ???

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

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    DiskDrive.read (file, desc, pos, cb)

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDrive => path == that.path
      case _ => false
    }

  override def toString = s"DiskDrive($id, $path, $config)"
}

private object DiskDrive {

  def read [P] (file: File, desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    guard (cb) {
      val buf = PagedBuffer (12)
      file.fill (buf, pos.offset, pos.length, callback (cb) { _ =>
        desc.ppag.unpickle (buf)
      })
    }

  def init (
      id: Int,
      path: Path,
      file: File,
      config: DiskDriveConfig,
      boot: BootBlock,
      logd: LogDispatcher,
      paged: PageDispatcher, cb: Callback [DiskDrive]) (
          implicit scheduler: Scheduler): Unit =

    guard (cb) {
      val alloc = SegmentAllocator.init (config)
      val logw = LogWriter.init (file, alloc, logd, delay (cb) { logw =>
        val pagew = PageWriter.init (id, file, config, alloc, paged)
        val disk = new DiskDrive (id, path, file, config, alloc, logw, pagew)
        disk.checkpoint (boot.gen)
        disk.checkpoint (boot, callback (cb) { _ =>
          disk
        })
      })
    }

  def init (items: Seq [(Path, File, DiskDriveConfig)], base: Int, boot: BootBlock,
      logd: LogDispatcher, paged: PageDispatcher, cb: Callback [Seq [DiskDrive]]) (
          implicit scheduler: Scheduler): Unit =

    guard (cb) {
      val latch = Callback.seq (items.size, cb)
      for (((path, file, config), i) <- items zipWithIndex) {
        DiskDrive.init (base + i, path, file, config, boot, logd, paged, latch)
      }}
}
