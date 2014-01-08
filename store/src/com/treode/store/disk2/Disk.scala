package com.treode.store.disk2

import java.nio.file.Path
import com.treode.async.Callback
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{pickle, unpickle}

class Disk (val path: Path, file: File, config: DiskConfig) {

  var gen = 0
  var engaged = false

  def init() {
    gen = 1
  }

  def engage() {
    engaged = true
  }

  def checkpoint (boot: BootBlock, cb: Callback [Unit]) {
    gen = boot.gen
    val superblock = SuperBlock (boot, config, gen)
    val buffer = PagedBuffer (12)
    pickle (SuperBlock.pickle, superblock, buffer)
    val pos = if ((gen & 1) == 0) 0 else SuperBlockBytes
    file.flush (buffer, pos, cb)
  }

  def recover (superblock: SuperBlock) {
    gen = superblock.gen
  }

  def close() = file.close()

  override def hashCode: Int = path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Disk => path == that.path
      case _ => false
    }

  override def toString = s"Disk($path, $gen, $config)"
}
