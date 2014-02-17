package com.treode.disk

import java.nio.file.Path

import com.treode.async.{Callback, defer}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

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

  def read (path: Path, file: File, cb: Callback [SuperBlocks]) (implicit config: DisksConfig) {
    defer (cb) {

      val buffer = PagedBuffer (config.superBlockBits+1)

      def unpickleSuperBlock (pos: Int): Option [SuperBlock] =
        try {
          buffer.readPos = pos
          Some (SuperBlock.pickler.unpickle (buffer))
        } catch {
          case e: Throwable => None
        }

      def unpickleSuperBlocks() {
        val sb1 = unpickleSuperBlock (0)
        val sb2 = unpickleSuperBlock (config.superBlockBytes)
        cb.pass (new SuperBlocks (path, file, sb1, sb2))
      }

      file.fill (buffer, 0, config.diskLeadBytes, new Callback [Unit] {
        def pass (v: Unit) = unpickleSuperBlocks()
        def fail (t: Throwable) = unpickleSuperBlocks()
      })
    }}}
