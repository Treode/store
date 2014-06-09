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
    if (useGen0) sb0.get else sb1.get

  override def toString = s"SuperBlocks($path, $sb0, $sb1)"
}

private object SuperBlocks {

  private def unpickle (buf: PagedBuffer): Option [SuperBlock] =
    try {
      Some (SuperBlock.pickler.unpickle (buf))
    } catch {
      case e: Throwable => None
    }

  def read (path: Path, file: File) (implicit config: DiskConfig): Async [SuperBlocks] =
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
      }}

  def chooseSuperBlock (superbs: Seq [SuperBlocks]): Boolean = {

    val sb0 = superbs.map (_.sb0) .flatten
    val sb1 = superbs.map (_.sb1) .flatten
    if (sb0.size == 0 && sb1.size == 0)
      throw new NoSuperBlocksException

    val gen0 = if (sb0.isEmpty) -1 else sb0.map (_.boot.gen) .max
    val n0 = sb0 count (_.boot.gen == gen0)
    val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
    val n1 = sb1 count (_.boot.gen == gen1)
    if (n0 != superbs.size && n1 != superbs.size)
      throw new InconsistentSuperBlocksException

    (n0 == superbs.size) && (gen0 > gen1 || n1 != superbs.size)
  }

  def verifyReattachment (superbs: Seq [SuperBlocks]) (implicit config: DiskConfig) {

    val useGen0 = chooseSuperBlock (superbs)
    val boot = superbs.head.superb (useGen0) .boot
    val expected = boot.disks.toSet
    val found = superbs.map (_.path) .toSet

    if (!(expected forall (found contains _))) {
      val missing = (expected -- found).toSeq.sorted
      throw new MissingDisksException (missing)
    }

    if (!(found forall (expected contains _))) {
      val extra = (found -- expected).toSeq.sorted
      throw new ExtraDisksException (extra)
    }}}
