package com.treode.disk

import java.nio.file.Path

import com.treode.async.Async
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.guard

private class SuperBlocks (
    val path: Path,
    val file: File,
    val sb0: Option [SuperBlock],
    val sb1: Option [SuperBlock]) {

  def superb (useGen0: Boolean): SuperBlock =
    if (useGen0) sb0.get else sb0.get

  override def toString = s"SuperBlocks($path, $sb0, $sb1)"
}

private object SuperBlocks {

  private def unpickle (buf: PagedBuffer): Option [SuperBlock] =
    try {
      Some (SuperBlock.pickler.unpickle (buf))
    } catch {
      case e: Throwable => None
    }

  def read (path: Path, file: File) (implicit config: DisksConfig): Async [SuperBlocks] =
    guard {
      val buf0 = PagedBuffer (config.superBlockBits)
      val buf1 = PagedBuffer (config.superBlockBits)
      for {
        _ <- file.deframe (checksum, buf0, 0) .recover {case _ => 0}
        _ <- file.deframe (checksum, buf1, config.superBlockBytes) .recover {case _ => 0}
      } yield {
        val sb0 = unpickle (buf0)
        val sb1 = unpickle (buf1)
        new SuperBlocks (path, file, sb0, sb1)
      }}}
