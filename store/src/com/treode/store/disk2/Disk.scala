package com.treode.store.disk2

import java.nio.file.Path
import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}

private class Disk (
    val path: Path,
    file: File,
    config: DiskConfig,
    scheduler: Scheduler,
    logd: LogDispatcher) {

  val free = new Allocator (config)
  val logw = new LogWriter (file, free, scheduler, logd)

  def init (cb: Callback [Unit]) {
    free.init()
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
        free.checkpoint (gen),
        logw.checkpoint (gen))
    val buffer = PagedBuffer (12)
    pickle (SuperBlock.pickle, superblock, buffer)
    val pos = if ((boot.gen & 1) == 0) 0 else SuperBlockBytes
    file.flush (buffer, pos, cb)
  }

  def recover (superblock: SuperBlock) {
    val gen = superblock.boot.gen
    free.recover (gen, superblock.free)
    logw.recover (gen, superblock.log)
  }

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Disk => path == that.path
      case _ => false
    }

  override def toString = s"Disk($path, $config)"
}
