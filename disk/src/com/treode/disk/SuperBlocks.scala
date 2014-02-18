package com.treode.disk

import java.nio.file.Path

import com.treode.async.{Async, Callback, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.guard

private class SuperBlocks (
    val path: Path,
    val file: File,
    val sb1: Option [SuperBlock],
    val sb2: Option [SuperBlock]) {

  def superb (useGen1: Boolean): SuperBlock =
    if (useGen1) sb1.get else sb2.get

  override def toString = s"SuperBlocks($path, $sb1, $sb2)"
}

private object SuperBlocks {

  def read (path: Path, file: File) (implicit config: DisksConfig): Async [SuperBlocks] =
    guard {

      val buffer = PagedBuffer (config.superBlockBits+1)

      def unpickleSuperBlock (pos: Int): Option [SuperBlock] =
        try {
          buffer.readPos = pos
          Some (SuperBlock.pickler.unpickle (buffer))
        } catch {
          case e: Throwable => None
        }

      for {
        _ <- file.fill (buffer, 0, config.diskLeadBytes)
      } yield {
        val sb1 = unpickleSuperBlock (0)
        val sb2 = unpickleSuperBlock (config.superBlockBytes)
        new SuperBlocks (path, file, sb1, sb2)
      }}}
