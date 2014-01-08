package com.treode.store.disk2

import java.nio.file.Path
import com.treode.async.Callback
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}

private class Disk (val path: Path, file: File, config: DiskConfig) {

  val free = new Allocator (config)

  def init() {
    free.init()
  }

  def engage() {
  }

  def checkpoint (boot: BootBlock, cb: Callback [Unit]) {
    val superblock = SuperBlock (boot, config, free.checkpoint (boot.gen))
    val buffer = PagedBuffer (12)
    pickle (SuperBlock.pickle, superblock, buffer)
    val pos = if ((boot.gen & 1) == 0) 0 else SuperBlockBytes
    file.flush (buffer, pos, cb)
  }

  def recover (superblock: SuperBlock) {
    free.recover (superblock.boot.gen, superblock.free)
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
