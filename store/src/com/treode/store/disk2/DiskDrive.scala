package com.treode.store.disk2

import java.nio.file.Path
import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}

private class DiskDrive (
    val path: Path,
    file: File,
    config: DiskDriveConfig,
    scheduler: Scheduler,
    logd: LogDispatcher) {

  val alloc = new SegmentAllocator (config)
  val logw = new LogWriter (file, alloc, scheduler, logd)

  def init (cb: Callback [Unit]) {
    alloc.init()
    logw.init (cb)
  }

  def engage() {
    logd.engage (logw)
  }

  def checkpoint (boot: BootBlock, cb: Callback [Unit]) {
    val gen = boot.gen
    val superblock = SuperBlock (
        boot,
        config,
        alloc.checkpoint (gen),
        logw.checkpoint (gen))
    val buffer = PagedBuffer (12)
    pickle (SuperBlock.pickle, superblock, buffer)
    val pos = if ((boot.gen & 1) == 0) 0 else SuperBlockBytes
    file.flush (buffer, pos, cb)
  }

  def recover (superblock: SuperBlock) {
    val gen = superblock.boot.gen
    alloc.recover (gen, superblock.alloc)
    logw.recover (gen, superblock.log)
  }

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDrive => path == that.path
      case _ => false
    }

  override def toString = s"DiskDrive($path, $config)"
}
